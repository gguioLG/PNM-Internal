# BAYESIAN NETWORK DEVELOPMENT BY METE.

# 1. Package calls
library(bnlearn)

# Creating an input dataframe for the algorithms.
BN_input <- mac_add_DS

# 2. Development of networks using different structure learning algorithms that are embedded in bnlearn package. (Constraint-based, Score-based, and Hybrid algorithms);
GS_Network = gs(BN_input)
HC_Network = hc(BN_input)
IAMB_Network = iamb(BN_input)
TABU_Network = tabu(BN_input)

# 3. Plotting the network graphs for visualization.
plot(GS_Network)
plot(HC_Network)
plot(IAMB_Network)
plot(TABU_Network)

# 4 Printing graphs to a pdf.
pdf("Liberty Global - Bayesian Network Graphs for MAC Features.pdf")

# Content of the pdf.
graphviz.plot(GS_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Grow-Shrink Algorithm Network", sub = "Mac Features Dataset")
graphviz.plot(HC_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Hill-Climb Algorithm Network", sub = "Mac Features Dataset")
graphviz.plot(IAMB_Network, highlight = NULL, layout = "dot", shape = "circle", main = "IAMB Algorithm Network", sub = "Mac Features Dataset")
graphviz.plot(TABU_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Tabu Algorithm Network", sub = "Mac Features Dataset")

# End writing to pdf.
dev.off()