import matplotlib.pyplot as plt
import numpy as np

fig, ax = plt.subplots(figsize=(8,6), dpi=600)

name = np.array(["Target Throughput: 8192", "Target Throughput: 16384"])
y = np.array([2982.6863120897547,8547.215252923279,805.3771813441851,14119.625485135135])

a = np.array([8547.215252923279,8874.723696576542])
b = np.array([2982.6863120897547,3282.6863120897547])

c = np.array([805.3771813441851,726.1886437836515])
d = np.array([14119.625485135135,15215.262512523642])


x = np.arange(2)
total_width, n = 5, 25
width = total_width / n
x = x - (total_width - width) / 2

plt.ylim((0,20000))
plt.bar(x, c,  width=width, label='Normal')
plt.bar(x + width, d, width=width, label='Straggler')
# plt.bar(x + 2 * width, c, width=width, label='c')
plt.legend()
plt.xticks(x+width/2,name)

ax.set_title('Latency of cloud deployment',fontsize=14,y=1.05)
ax.set_ylabel('Latency / ms',fontsize = 14,color = 'black',rotation=90)

plt.text(x[0],c[0],int(c[0]),ha='center',va='bottom',fontsize=12)
plt.text(x[1],c[1],int(c[1]),ha='center',va='bottom',fontsize=12)
plt.text(x[0]+width,d[0],int(d[0]),ha='center',va='bottom',fontsize=12)
plt.text(x[1]+width,d[1],int(d[1]),ha='center',va='bottom',fontsize=12)


fig.savefig('scripts/analyze/latency.png', dpi=600)
