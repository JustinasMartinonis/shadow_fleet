import matplotlib.pyplot as plt
import numpy as np

workers = np.array([1, 4, 8, 12])
times = np.array([100.73, 101.5, 101.04, 100.9])          

avg_cpu = np.array([0.22, 0.2, 0.21, 0.21])      

T_effective_1 = times[0] / avg_cpu[0]  
print(f"Measured 1-worker time: {times[0]:.2f} s")
print(f"Effective single-worker time (if 100% CPU bound): {T_effective_1:.2f} s")

observed_speedup = times[0] / times
corrected_speedup = T_effective_1 / times

# Estimate P using Amdahl's law from last worker
N = workers[-1]
S_N = corrected_speedup[-1]
P = (1 - 1/S_N) / (1 - 1/N)
print(f"Estimated algorithmic parallel fraction P ≈ {P:.3f}")

# Amdahl curve 
N_curve = np.arange(1, 13)  
theoretical_speedup = 1 / ((1 - P) + P / N_curve)

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
