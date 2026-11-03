"""Chapter 13: where this session's memory goes. Computes the unified region and compares it with the Spark UI."""
import json
import urllib.request

from saffron.session import start

spark = start("ch13-memory")
sc = spark.sparkContext


def ui(path):
    """One page of the Spark UI's REST API, as Python data."""
    with urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}") as page:
        return json.load(page)


SETTINGS = (("spark.driver.memory", "1g"), ("spark.memory.fraction", "0.6"), ("spark.memory.storageFraction", "0.5"))
print([spark.conf.get(key, default) for key, default in SETTINGS])
heap = spark._jvm.java.lang.Runtime.getRuntime().maxMemory() / 2**20
usable = heap - 300
unified = 0.6 * usable
print(f"heap {heap:.0f}, usable {usable:.0f}, unified {unified:.1f} = "
      f"storage {unified / 2:.1f} + execution {unified / 2:.1f}, user {usable - unified:.1f}")
print("Executors tab:", round(ui("executors")[0]["maxMemory"] / 2**20, 1))

# More checks, not printed in the chapter: the 10 GiB heap by the same arithmetic.
print("--- more checks")
usable = 10 * 1024 - 300
print(f"10 GiB: usable {usable}, unified {0.6 * usable:.1f}, user {usable - 0.6 * usable:.1f}")
spark.stop()
