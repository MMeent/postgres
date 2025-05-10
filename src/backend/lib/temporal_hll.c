/*-------------------------------------------------------------------------
 *
 * temporal_hll.c
 *	  HyperLogLog-based cardinality estimator in 'X most recent epochs'
 *
 * Portions Copyright (c) 2014-2025, PostgreSQL Global Development Group
 *
 * Based on Hideaki Ohno's C++ implementation.  This is probably not ideally
 * suited to estimating the cardinality of very large sets;  in particular, we
 * have not attempted to further optimize the implementation as described in
 * the Heule, Nunkesser and Hall paper "HyperLogLog in Practice: Algorithmic
 * Engineering of a State of The Art Cardinality Estimation Algorithm".
 *
 * The copyright terms of Ohno's original version (the MIT license) follow.
 *
 * IDENTIFICATION
 *	  src/backend/lib/temporal_hll.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * Copyright (c) 2013 Hideaki Ohno <hide.o.j55{at}gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the 'Software'), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include "postgres.h"

#include <math.h>

#include "lib/temporal_hll.h"
#include "port/pg_bitutils.h"

#define POW_2_32			(4294967296.0)
#define NEG_POW_2_32		(-4294967296.0)

static inline uint8 rho(uint32 x, uint8 b);

static uint8 error_to_bwidth(double error);
static void clearTempHLLState(TempHLLState *state);
static void mergeTempIntoTemporalHLL(TemporalHLLState *sstate,
									 TempHLLState *tstate);

#define SUBREG_OFFSET(nregs, regno, bitcount) ((Size) (bitcount - 1) * (nregs) + (Size) (regno))

Size
TemporalHLLSize(uint8 bwidth)
{
	Size		nRegisters = (Size) 1 << bwidth;
	Size		nSubRegisters = (Size) (33 - bwidth);
	Size		totSubRegisters = nSubRegisters * nRegisters;

	return offsetof(TemporalHLLState, registers) + \
		sizeof(TempHLLEpochRegister) * totSubRegisters;
}

Size
TemporalHLLSizeError(double error)
{
	return TemporalHLLSize(error_to_bwidth(error));
}

Size
TempHLLSize(uint8 bwidth)
{
	Size		nRegisters = (Size) 1 << bwidth;

	return offsetof(TempHLLState, registers) + \
		sizeof(uint8) * nRegisters;
}

Size
TempHLLSizeError(double error)
{
	return TempHLLSize(error_to_bwidth(error));
}

Size
TempHLLQuerySize(TemporalHLLState *sstate)
{
	return TempHLLSize(sstate->registerWidth);
}

/*
 * Initialize HyperLogLog track state, by bit width
 *
 * bwidth is bit width (so register size will be 2 to the power of bwidth).
 * Must be between 4 and 16 inclusive.
 */
void
initTemporalHLL(TemporalHLLState *cstate, uint8 bwidth)
{
	double		alpha;
	Size		totSubRegisters;

	if (bwidth < 4 || bwidth > 16)
		elog(ERROR, "bit width must be between 4 and 16 inclusive");

	cstate->registerWidth = bwidth;
	cstate->nRegisters = (Size) 1 << bwidth;
	cstate->nSubRegisters = (Size) (33 - bwidth);

	totSubRegisters = cstate->nRegisters * cstate->nSubRegisters;

	for (int i = 0; i < totSubRegisters; i++)
	{
		pg_atomic_init_u32(&cstate->registers[i], 0);
	}

	/*
	 * "alpha" is a value that for each possible number of registers (m) is
	 * used to correct a systematic multiplicative bias present in m ^ 2 Z (Z
	 * is "the indicator function" through which we finally compute E,
	 * estimated cardinality).
	 */
	switch (cstate->nRegisters)
	{
		case 16:
			alpha = 0.673;
			break;
		case 32:
			alpha = 0.697;
			break;
		case 64:
			alpha = 0.709;
			break;
		default:
			alpha = 0.7213 / (1.0 + 1.079 / cstate->nRegisters);
	}

	/*
	 * Precalculate alpha m ^ 2, later used to generate "raw" HyperLogLog
	 * estimate E
	 */
	cstate->alphaMM = alpha * cstate->nRegisters * cstate->nRegisters;
}

static uint8
error_to_bwidth(double error)
{
	uint8		bwidth = 4;

	while (bwidth < 16)
	{
		double		m = (Size) 1 << bwidth;

		if (1.04 / sqrt(m) < error)
			break;
		bwidth++;
	}

	return bwidth;
}

/*
 * Initialize HyperLogLog track state, by error rate
 *
 * Instead of specifying bwidth (number of bits used for addressing the
 * register), this method allows sizing the counter for particular error
 * rate using a simple formula from the paper:
 *
 *	 e = 1.04 / sqrt(m)
 *
 * where 'm' is the number of registers, i.e. (2^bwidth). The method
 * finds the lowest bwidth with 'e' below the requested error rate, and
 * then uses it to initialize the counter.
 *
 * As bwidth has to be between 4 and 16, the worst possible error rate
 * is between ~25% (bwidth=4) and 0.4% (bwidth=16).
 */
void
initTemporalHLLError(TemporalHLLState *cState, double error)
{
	initTemporalHLL(cState, error_to_bwidth(error));
}

void
initTempHLL(TemporalHLLState *from, TempHLLState *tstate)
{
	tstate->nRegisters = from->nRegisters;
	tstate->registerWidth = from->registerWidth;
	tstate->alphaMM = from->alphaMM;

	clearTempHLLState(tstate);
}

/*
 * Merge the registers of TempHLLState into TemporalHLLState.
 */
static void
mergeTempIntoTemporalHLL(TemporalHLLState *sstate, TempHLLState *tstate)
{
	TmpHLLEpoch		epoch = tstate->generation;

	Assert(sstate->nRegisters == tstate->nRegisters);

	for (int i = 0; i < tstate->nRegisters; i++)
	{
		int32		count = (int32) tstate->registers[i];
		TempHLLEpochRegister *reg;
		TmpHLLEpoch	regepoch;

		if (count == 0)
			continue;

		reg = &sstate->registers[SUBREG_OFFSET(tstate->nRegisters, i, count)];
		regepoch = pg_atomic_read_u32(reg);

		while (regepoch < epoch)
		{
			(void) pg_atomic_compare_exchange_u32(reg, &regepoch, epoch);
		}
	}
}

static void
clearTempHLLState(TempHLLState *state)
{
	state->generation = 0;
	memset(state->registers, (uint8) -1, state->nRegisters);
}

/*
 * Adds element to the estimator, from caller-supplied hash.
 *
 * It is critical that the hash value passed be an actual hash value, typically
 * generated using hash_any().  The algorithm relies on a specific bit-pattern
 * observable in conjunction with stochastic averaging.  There must be a
 * uniform distribution of bits in hash values for each distinct original value
 * observed.
 */
void
addTempHLL(TemporalHLLState *sstate, TempHLLState *tstate,
		   uint32 hash, TmpHLLEpoch epoch)
{
	uint8		count;
	uint32		index;

	if (tstate->generation != epoch)
	{
		mergeTempIntoTemporalHLL(sstate, tstate);
		clearTempHLLState(tstate);
		tstate->generation = epoch;
	}

	/* Use the first "k" (registerWidth) bits as a zero based index */
	index = hash >> (BITS_PER_BYTE * sizeof(uint32) - tstate->registerWidth);

	/* Compute the rank of the remaining 32 - "k" (registerWidth) bits */
	count = rho(hash << tstate->registerWidth,
				BITS_PER_BYTE * sizeof(uint32) - tstate->registerWidth);

	tstate->registers[index] = Max(tstate->registers[index], count);
}

/*
 * Add a value directly to the TemporalHLLState.
 *
 * If the state is shared it's probably better to use a separate TempHLLState
 */
void
addTemporalHLL(TemporalHLLState *sstate, uint32 hash, TmpHLLEpoch epoch)
{
	int			index;
	int			count;
	TempHLLEpochRegister *reg;
	TmpHLLEpoch	regepoch;

	/* Use the first "k" (registerWidth) bits as a zero based index */
	index = hash >> (BITS_PER_BYTE * sizeof(uint32) - sstate->registerWidth);

	/* Compute the rank of the remaining 32 - "k" (registerWidth) bits */
	count = rho(hash << sstate->registerWidth,
				BITS_PER_BYTE * sizeof(uint32) - sstate->registerWidth);

	reg = &sstate->registers[SUBREG_OFFSET(sstate->nRegisters, index, count)];
	regepoch = pg_atomic_read_u32(reg);

	while (regepoch < epoch)
	{
		(void) pg_atomic_compare_exchange_u32(reg, &regepoch, epoch);
	}
}

void
queryTemporalHLL(TemporalHLLState *source, TemporalHLLQuery *query)
{
	int			nregs = source->nRegisters;
	int			subregs = source->nSubRegisters;
	int			mincount = subregs;
	TmpHLLEpoch	qepoch = query->generation;

	/*
	 * If all registers in the query already have >= 4 bits, we won't have to
	 * look at the subregisters for 5 bits or more, reducing the amount of
	 * memory accessed while querying the data.
	 */
	for (int reg = 0; reg < nregs; reg++)
	{
		mincount = Min(mincount, query->registers[reg]);
	}

	for (int subreg = mincount + 1; subreg <= source->nSubRegisters; subreg++)
	{
		for (int reg = 0; reg < nregs; reg++)
		{
			TempHLLEpochRegister *hllreg;

			if (query->registers[reg] >= subreg)
				continue;

			hllreg = &source->registers[SUBREG_OFFSET(nregs, reg, subreg)];

			if (pg_atomic_read_u32(hllreg) >= qepoch)
				query->registers[reg] = (int8) subreg + 1;
		}
	}
}

void
queryTempHLL(TempHLLState *source, TemporalHLLQuery *query)
{
	int			nregs = source->nRegisters;

	if (source->generation < query->generation)
		return;

	for (int reg = 0; reg < nregs; reg++)
	{
		query->registers[reg] = Max(query->registers[reg],
									source->registers[reg]);
	}
}

/*
 * Estimates cardinality, based on elements added so far
 */
double
estimateTemporalHLLQuery(TemporalHLLQuery *query)
{
	double		result;
	double		sum = 0.0;
	int			i;

	for (i = 0; i < query->nRegisters; i++)
	{
		int count = Max(0, query->registers[i]);
		sum += 1.0 / pow(2.0, count);
	}

	/* result set to "raw" HyperLogLog estimate (E in the HyperLogLog paper) */
	result = query->alphaMM / sum;

	if (result <= (5.0 / 2.0) * query->nRegisters)
	{
		/* Small range correction */
		int			zero_count = 0;

		for (i = 0; i < query->nRegisters; i++)
		{
			if (query->registers[i] == 0)
				zero_count++;
		}

		if (zero_count != 0)
			result = query->nRegisters * log((double) query->nRegisters /
											 zero_count);
	}
	else if (result > (1.0 / 30.0) * POW_2_32)
	{
		/* Large range correction */
		result = NEG_POW_2_32 * log(1.0 - (result / POW_2_32));
	}

	return result;
}

/*
 * Worker for addHyperLogLog().
 *
 * Calculates the position of the first set bit in first b bits of x argument
 * starting from the first, reading from most significant to least significant
 * bits.
 *
 * Example (when considering fist 10 bits of x):
 *
 * rho(x = 0b1000000000)   returns 1
 * rho(x = 0b0010000000)   returns 3
 * rho(x = 0b0000000000)   returns b + 1
 *
 * "The binary address determined by the first b bits of x"
 *
 * Return value "j" used to index bit pattern to watch.
 */
static inline uint8
rho(uint32 x, uint8 b)
{
	uint8		j = 1;

	if (x == 0)
		return b + 1;

	j = 32 - pg_leftmost_one_pos32(x);

	if (j > b)
		return b + 1;

	return j;
}
