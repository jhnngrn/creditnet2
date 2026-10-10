package creditnet

import "math/bits"

// Demurrage rate is promille per year. 10 = 1%.
// decay = |balance| * rate * elapsed_s / (1000 * seconds_per_year)
const (
	rateBase    = 1000
	secsPerYear = 365 * 24 * 3600 // 31_536_000
)

// mulDiv computes a * b / c with 128-bit intermediate to avoid overflow.
func mulDiv(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	if hi == 0 {
		return lo / c
	}
	if hi >= c {
		return ^uint64(0)
	}
	q, _ := bits.Div64(hi, lo, c)
	return q
}

func demurrageDecay(absBalance uint64, rate uint64, elapsedSecs int64) uint64 {
	if rate == 0 || elapsedSecs <= 0 || absBalance == 0 {
		return 0
	}
	perYear := mulDiv(absBalance, rate, rateBase)
	decay := mulDiv(perYear, uint64(elapsedSecs), secsPerYear)
	if decay > absBalance {
		return absBalance
	}
	return decay
}

// MaterializeDemurrage updates Balance to reflect decay up to timestamp ts.
// Must be called with a shared timestamp (from sync) for consistency.
func (p *Peer) MaterializeDemurrage(ts int64) {
	if p.DemurrageTime == 0 || p.Balance == 0 {
		p.DemurrageTime = ts
		return
	}
	elapsed := ts - p.DemurrageTime
	if elapsed <= 0 {
		return
	}
	var rate int64
	if p.Balance > 0 {
		rate = p.Demurrage // peer owes me — my rate
	} else {
		rate = p.DemurrageIn // I owe peer — peer's rate
	}
	if rate <= 0 {
		p.DemurrageTime = ts
		return
	}
	abs := p.Balance
	if abs < 0 {
		abs = -abs
	}
	decay := demurrageDecay(uint64(abs), uint64(rate), elapsed)
	if decay >= uint64(abs) {
		p.Balance = 0
	} else if p.Balance > 0 {
		p.Balance -= int64(decay)
	} else {
		p.Balance += int64(decay)
	}
	p.DemurrageTime = ts
}

// EffectiveBalance returns balance after demurrage decay at the given time
// without modifying the peer. Used for reads and validation.
func (p *Peer) EffectiveBalance(nowSecs int64) int64 {
	if p.DemurrageTime == 0 || p.Balance == 0 {
		return p.Balance
	}
	elapsed := nowSecs - p.DemurrageTime
	if elapsed <= 0 {
		return p.Balance
	}
	var rate int64
	if p.Balance > 0 {
		rate = p.Demurrage
	} else {
		rate = p.DemurrageIn
	}
	if rate <= 0 {
		return p.Balance
	}
	abs := p.Balance
	if abs < 0 {
		abs = -abs
	}
	decay := demurrageDecay(uint64(abs), uint64(rate), elapsed)
	if decay >= uint64(abs) {
		return 0
	}
	if p.Balance > 0 {
		return p.Balance - int64(decay)
	}
	return p.Balance + int64(decay)
}

// AddBalance materializes demurrage at ts, then applies delta.
func (p *Peer) AddBalance(delta int64, ts int64) {
	p.MaterializeDemurrage(ts)
	p.Balance += delta
}
