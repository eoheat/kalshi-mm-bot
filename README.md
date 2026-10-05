# kalshi-mm-bot

Infrastructure for trading Kalshi's hourly BTC and ETH price markets: an
exchange client, a live WebSocket order-book feed, a tape recorder, and a
replay backtester.

This is the public half of the project. The pricing, sizing and signal code,
and the tests that cover it, are in a private repository and available on request.

## Layout

    src/kalshi_client.py   REST client: signed requests, orders, positions, fills
    src/shards.py          moves collateral between Kalshi's exchange shards
    src/risk.py            position, exposure and loss limits
    src/feed/              WebSocket order books, Deribit index price, tape recorder
    src/replay/            replay backtester with latency and queue-position models
    paper/broker.py        paper broker: simulated fills and settlement
    tests/                 37 offline tests, no network or keys needed

## Run the tests

    pip install -r requirements.txt pytest
    python -m pytest tests -q

## Record and replay

    cp .env.example .env        # add your own Kalshi API key
    python -m src.feed.record --env prod --series KXBTC KXETH --out data/tape
    python -m src.replay.run 'data/tape/tape-*.jsonl.gz' --latency-ms 100 3000 --queue back front

## Known issue

When more than 100 markets are subscribed, the feed can treat Kalshi's batch
acknowledgements as a sequence gap and resubscribe only the first batch.
