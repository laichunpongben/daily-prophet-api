import asyncio

from dailyprophet.readers.reader import Reader


async def main():
    reader = Reader("BL")
    out = await reader.async_sample(50)
    return out


if __name__ == "__main__":
    out = asyncio.run(main())
    print(out)
