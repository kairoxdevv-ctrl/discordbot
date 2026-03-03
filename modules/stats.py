VERSION = "1.1.0"
def build_stats(guild, bot_latency_ms: int) -> dict:
    if guild is None:
        return {
            "member_count": 0,
            "online_count": 0,
            "boost_level": 0,
            "latency_ms": bot_latency_ms,
        }

    online = 0
    for m in guild.members:
        if str(getattr(m, "status", "offline")) != "offline":
            online += 1

    return {
        "member_count": guild.member_count or len(guild.members),
        "online_count": online,
        "boost_level": int(getattr(guild, "premium_tier", 0) or 0),
        "latency_ms": bot_latency_ms,
    }
