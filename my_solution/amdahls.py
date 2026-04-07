import matplotlib.pyplot as plt
import numpy as np

# --- Input data from your run ---
# Update this array if your benchmark tested other core counts (e.g., [1, 2, 4, 12])
workers = np.array([1, 12])

# Your actual benchmark times
times = np.array([329.65, 226.60])          

# Estimated average CPU fraction (0-1). 
# Single core is highly I/O bound (~35% CPU efficiency). Max cores pushes harder (~85%).
avg_cpu = np.array([0.24, 0.34])      

# --- Normalize single-worker time by CPU usage ---
T_effective_1 = times[0] / avg_cpu[0]  
print(f"Measured 1-worker time: {times[0]:.2f} s")
print(f"Effective single-worker time (if 100% CPU bound): {T_effective_1:.2f} s")

# --- Calculate corrected speedups ---
observed_speedup = times[0] / times
corrected_speedup = T_effective_1 / times

# --- Estimate parallel fraction P using Amdahl's law from last worker ---
N = workers[-1]
S_N = corrected_speedup[-1]
P = (1 - 1/S_N) / (1 - 1/N)
print(f"Estimated algorithmic parallel fraction P ≈ {P:.3f}")

# --- Generate theoretical Amdahl curve ---
N_curve = np.arange(1, 13)  # 1 to 12 workers
theoretical_speedup = 1 / ((1 - P) + P / N_curve)

# --- Plot results ---
plt.figure(figsize=(10,6))
plt.plot(workers, observed_speedup, 'o-', label='Observed Speedup (Raw Time)', linewidth=2, color='gray')
plt.plot(workers, corrected_speedup, 's-', label='Corrected Speedup (CPU Normalized)', linewidth=2, color='steelblue')
plt.plot(N_curve, theoretical_speedup, '--', label=f"Amdahl's Theoretical (P={P:.1%})", linewidth=2, color='tomato')
plt.xlabel("Number of Worker Cores")
plt.ylabel("Speedup (x)")
plt.title("Observed vs Corrected Speedup (Overcoming I/O Bottleneck)")
plt.grid(True)
plt.legend()
plt.show()