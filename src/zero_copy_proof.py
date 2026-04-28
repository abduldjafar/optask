import polars as pl
import time

print("Generating 20 million rows of data...")
df = pl.DataFrame({
    "id": pl.int_range(0, 20_000_000, eager=True),
    "value": pl.int_range(0, 20_000_000, eager=True) * 1.5
})
print(f"Dataframe size in memory: {df.estimated_size('mb'):.2f} MB\n")

print("==================================================")
print(" 1. ZERO-COPY (Polars <-> PyArrow)")
print("==================================================")
start = time.perf_counter()
arrow_table = df.to_arrow()
end = time.perf_counter()
print(f"Time taken: {(end - start) * 1000:.4f} ms\n")

print("==================================================")
print(" 2. DEEP-COPY (Polars -> Pandas)")
print("==================================================")
try:
    start = time.perf_counter()
    pandas_df = df.to_pandas()
    end = time.perf_counter()
    print(f"Time taken: {(end - start) * 1000:.4f} ms")
    print("(Ini lambat karena data RAM diduplikat total ke format struktur memori NumPy/Pandas yang berbeda)\n")
except Exception as e:
    print(f"Pandas not installed: {e}")

print("==================================================")
print(" 3. PURE PYTHON DEEP-COPY (Polars -> List of Dicts)")
print("==================================================")
# We test with 1 million rows instead of 20 million to prevent the server from crashing or running out of RAM
df_small = df.head(1_000_000)
start = time.perf_counter()
python_list = df_small.to_dicts()
end = time.perf_counter()

# Extrapolate to 20 million
extrapolated_time = (end - start) * 1000 * 20
print(f"Time taken (Extrapolated for 20 Juta baris): {extrapolated_time:.4f} ms")
print("(Ini super lambat karena data dipecah satu per satu menjadi object Python standard yang memakan memori luar biasa boros!)")
