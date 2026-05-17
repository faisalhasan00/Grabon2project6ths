"""Run autonomous crawler surveillance: python crawler_service.py"""
import asyncio

from agents.crawler.service import main

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSurveillance stopped.")
