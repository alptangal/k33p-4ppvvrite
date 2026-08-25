import asyncio


async def getBasic(guild):
    for category in guild.categories:
        if "streamlit" in category.name:
            print(category.name)
            streamlitCate = category
            for channel in category.channels:
                if "urls" in channel.name:
                    urlsCh = channel
                elif "raw" in channel.name:
                    rawCh = channel
                    print(rawCh.id)

    return {
        "streamlitCate": streamlitCate if "streamlitCate" in locals() else None,
        "urlsCh": urlsCh if "urlsCh" in locals() else None,
        "rawCh": rawCh if "rawCh" in locals() else None,
    }
