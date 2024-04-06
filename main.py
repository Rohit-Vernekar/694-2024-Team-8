from src.cache import Cache
import time

test_cache = Cache(cache_path="/Users/rohitvernekar/Desktop/cache.pkl")

for i in range(200):
    test_cache.put(i, i**5)
    time.sleep(1)
    for j in range(i):
        print(test_cache.get(j))

# for key, value in test_cache._data.items():
#     print(key, value)