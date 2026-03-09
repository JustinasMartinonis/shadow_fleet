import matplotlib.pyplot as plt
import numpy as np
# This code is used for measured and theoretical speedup calculations - originally the speedup was observed to be above theoretical
# and because of the python gli overhead it was decided to implement a bonus computation of the corrected speedup

# --- Input data from your run ---
workers = np.array([1, 3, 6])
times = np.array([25.0, 5.5, 3.7])          # observed runtimes (seconds)
avg_cpu = np.array([0.27, 0.51, 0.84])      # average CPU fraction (0–1)

# --- Normalize single-worker time by CPU usage ---
T_effective_1 = times[0] / avg_cpu[0]  # adjusted time assuming full CPU
print(f"Effective single-worker time: {T_effective_1:.2f} s")

# --- Calculate corrected speedups ---
observed_speedup = times[0] / times
corrected_speedup = T_effective_1 / times

# --- Estimate parallel fraction P using Amdahl's law from last worker ---
N = workers[-1]
S_N = corrected_speedup[-1]
P = (1 - 1/S_N) / (1 - 1/N)
print(f"Estimated parallel fraction P ≈ {P:.3f}")

# --- Generate theoretical Amdahl curve ---
N_curve = np.arange(1, 21)  # 1 to 20 workers
theoretical_speedup = 1 / ((1 - P) + P / N_curve)

# --- Plot results ---
plt.figure(figsize=(10,6))
plt.plot(workers, observed_speedup, 'o-', label='Observed speedup', linewidth=2)
plt.plot(workers, corrected_speedup, 's-', label='Corrected speedup (CPU normalized)', linewidth=2)
#plt.plot(N_curve, theoretical_speedup, '--', label=f'Amdahl theoretical (P={P:.3f})', linewidth=2)
plt.xlabel("Number of workers")
plt.ylabel("Speedup")
plt.title("Observed vs Corrected Speedup and Amdahl's Law")
plt.grid(True)
plt.legend()
plt.show()