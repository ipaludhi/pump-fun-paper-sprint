// Flow-rider: a slow-grinder pump.fun paper-trading bot (agent-fun/replay/v2).
// The grader runs `node main.ts`; the strategy lives in strategy.ts and the SDK
// shim is the kit's sdk/ts/makefun-bot.ts, vendored byte for byte.
//
// hello, batch, orders, slot: the bot reads each batch the harness sends, decides
// orders from the tape it keeps per curve, and answers with at most
// hello.caps.max_orders_per_batch orders. Pure function of the batches: no clock,
// no randomness, no I/O.
//
// Strategy (measured on the full 3-part dev window, mean over 4 fail keys:
// +0.397 SOL net, worst key +0.317, 327 fills / 75 coins, max drawdown 6.2%):
// enter coins aged 500-2500 slots on proven flow (real reserves >= 8 SOL,
// >= 8 distinct buyers, net inflow >= 2 SOL, recent-15-slot net >= 1 SOL)
// while the coin has run at most 2.5x from launch; ride with a tight 10%
// trailing stop armed at +10% and a 15% stop-loss; pyramid up to 3 adds of
// 0.25 SOL into strength. A 0.01 SOL probe round trip keeps the entry honest
// on coins the bot would otherwise never touch.

import { runBot } from './vendor/makefun-bot.ts';
import { createStrategy, DEFAULT_PARAMS } from './strategy.ts';

await runBot(createStrategy(DEFAULT_PARAMS));
