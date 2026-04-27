"""
Node functions for the web scraping pipeline.

Demonstrates:
- Async node functions on thread pools (shared event loop)
- ctx.state for caching expensive data across retries
- ctx.log for structured logging
- ctx.vars for runtime configuration
- Simulated failures for retry testing

Port names are derived from function parameter names (from_function factory).
"""

import asyncio
import time
import random


# ---------------------------------------------------------------------------
# Stage 1: Configuration
# ---------------------------------------------------------------------------

def load_config(ctx) -> dict:
    """Load pipeline configuration from node variables.

    Demonstrates: ctx.vars, ctx.print, run_on_init
    Output port: 'out' (single return)
    """
    news_url = ctx.vars["news_url"]
    weather_url = ctx.vars["weather_url"]
    stocks_url = ctx.vars["stocks_url"]
    # Node variables are already resolved by the time they reach the node.
    # If they're NodeVariable objects, call resolve_value(); if already strings, use directly.
    if hasattr(news_url, "resolve_value"):
        news_url = news_url.resolve_value()
        weather_url = weather_url.resolve_value()
        stocks_url = stocks_url.resolve_value()

    config = {
        "news_url": news_url,
        "weather_url": weather_url,
        "stocks_url": stocks_url,
        "timestamp": time.time(),
    }

    ctx.print(f"Config loaded: {len(config)} settings")
    return config


def validate_config(ctx, config: dict) -> dict:
    """Validate that the config has all required fields.

    Input port: 'config'. Output port: 'out'.
    """
    required = {"news_url", "weather_url", "stocks_url"}
    missing = required - set(config.keys())
    if missing:
        raise ValueError(f"Missing config fields: {missing}")

    ctx.print(f"Config validated: all {len(required)} required fields present")
    return config


# ---------------------------------------------------------------------------
# Stage 2: Setup (parallel path — demonstrates depends_on)
# ---------------------------------------------------------------------------

def setup_output(ctx, trigger) -> dict:
    """Prepare the output directory for export.

    Demonstrates: depends_on (waits for load_config before starting).
    This node is NOT connected to load_config by a data edge — the
    ordering is enforced purely by depends_on.

    Input port: 'trigger'. Output port: 'out'.
    """
    output_dir = "/tmp/netrun_scraping_output"
    ctx.print(f"Output directory prepared: {output_dir}")
    return {"output_dir": output_dir, "ready": True}


# ---------------------------------------------------------------------------
# Stage 3: Async scrapers (thread pool, resources, retries)
# ---------------------------------------------------------------------------

async def scrape_news(ctx, config: dict) -> dict:
    """Scrape news headlines from a simulated news API.

    Demonstrates: async node function on thread pool, resources (http_conn),
    structured logging (ctx.log)
    Input port: 'config'. Output port: 'out'.
    """
    url = config["news_url"]
    ctx.print(f"Scraping news from {url}...")

    # Simulate async HTTP request (works on thread pools via shared event loop)
    await asyncio.sleep(0.15)

    headlines = [
        {"title": "Tech stocks surge on AI optimism", "source": "Reuters"},
        {"title": "Climate summit reaches breakthrough", "source": "AP"},
        {"title": "New space telescope captures distant galaxy", "source": "NASA"},
    ]

    ctx.log("scrape_complete", source="news", url=url, items=len(headlines))
    ctx.print(f"News: {len(headlines)} headlines scraped")
    return {"source": "news", "data": headlines}


async def scrape_weather(ctx, config: dict) -> dict:
    """Scrape weather data from a simulated weather API.

    Demonstrates: async node function on thread pool, resources (http_conn)
    Input port: 'config'. Output port: 'out'.
    """
    url = config["weather_url"]
    ctx.print(f"Scraping weather from {url}...")

    await asyncio.sleep(0.12)

    weather = [
        {"city": "London", "temp_c": 14, "condition": "cloudy"},
        {"city": "Tokyo", "temp_c": 22, "condition": "sunny"},
        {"city": "New York", "temp_c": 18, "condition": "rain"},
    ]

    ctx.log("scrape_complete", source="weather", url=url, items=len(weather))
    ctx.print(f"Weather: {len(weather)} cities scraped")
    return {"source": "weather", "data": weather}


async def scrape_stocks(ctx, config: dict) -> dict:
    """Scrape stock prices from a simulated stock API.

    Demonstrates: async on thread pool, resources, retries with intermittent
    failure. First attempt fails ~50% of the time to exercise retry logic.
    Input port: 'config'. Output port: 'out'.
    """
    url = config["stocks_url"]
    ctx.print(f"Scraping stocks from {url} (attempt {ctx.retry_count + 1})...")

    await asyncio.sleep(0.10)

    # Simulate intermittent API failure on first attempt
    if ctx.retry_count == 0 and random.random() < 0.5:
        ctx.print("Stock API returned 503 — will retry")
        raise ConnectionError(f"HTTP 503 from {url}")

    stocks = [
        {"symbol": "AAPL", "price": 187.50, "change": +1.2},
        {"symbol": "GOOGL", "price": 141.80, "change": -0.5},
        {"symbol": "MSFT", "price": 378.20, "change": +2.1},
    ]

    ctx.log("scrape_complete", source="stocks", url=url, items=len(stocks),
            retries=ctx.retry_count)
    ctx.print(f"Stocks: {len(stocks)} prices scraped")
    return {"source": "stocks", "data": stocks}


# ---------------------------------------------------------------------------
# Stage 4: Analysis (retries + ctx.state)
# ---------------------------------------------------------------------------

def analyze(ctx, joined: dict) -> dict:
    """Analyze combined scraping results.

    Demonstrates: ctx.state (caches expensive computation across retries),
    structured logging (ctx.log), retries.

    On first attempt, builds an analysis from the joined data and stores it
    in ctx.state. On retry, reuses the cached result instead of recomputing.

    Note: ctx.state works best with thread pools (same process, shared memory).
    With multiprocess pools, the dict is pickled per retry — mutations within
    a single attempt work, but cross-retry persistence requires thread pools.

    Input port: 'joined'. Output port: 'out'.
    """
    # Use ctx.state to cache the expensive computation across retries
    if "analysis_result" not in ctx.state:
        ctx.print("Computing analysis (first time — will cache in ctx.state)...")

        total_items = 0
        sources = []
        summary = {}

        for port_name, value in joined.items():
            if isinstance(value, dict) and "data" in value:
                total_items += len(value["data"])
                sources.append(value["source"])
                summary[value["source"]] = len(value["data"])

        result = {
            "total_items": total_items,
            "sources": sources,
            "source_count": len(sources),
            "summary": summary,
        }

        ctx.state["analysis_result"] = result
        ctx.print(f"Analysis cached: {total_items} items from {len(sources)} sources")
    else:
        result = ctx.state["analysis_result"]
        ctx.print(f"Reusing cached analysis from ctx.state (retry {ctx.retry_count})")

    ctx.log("analysis_complete",
            total_items=result["total_items"],
            sources=result["source_count"],
            retry_count=ctx.retry_count)

    return result


# ---------------------------------------------------------------------------
# Stage 5: Export (depends_on setup_output)
# ---------------------------------------------------------------------------

def export_report(ctx, report: dict) -> dict:
    """Export the analysis report.

    Demonstrates: depends_on (waits for setup_output before starting),
    output queue (results pushed to 'results' queue).
    Input port: 'report'. Output port: 'out'.
    """
    ctx.print(f"Exporting report: {report['total_items']} items from {report['source_count']} sources")
    ctx.print(f"  Summary: {report['summary']}")
    return report
