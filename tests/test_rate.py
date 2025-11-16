import time
from crawlerlib.rate import RateLimiter


def test_rate_limiter_waits():
    timeline = [0.0]

    def now():
        return timeline[0]

    sleeps = []

    def sleep(s):
        sleeps.append(s)
        timeline[0] += s

    rl = RateLimiter(0.5, now=now, sleep=sleep)
    rl.wait_turn("a.com")
    assert sleeps == []  # first call no wait
    rl.wait_turn("a.com")
    assert sleeps and 0.49 <= sleeps[-1] <= 0.5

