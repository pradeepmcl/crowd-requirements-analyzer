sim = csvread('similarity_tags_health.csv', 0, 0);
ids = csvread('reqIds_tags_health.csv', 0, 0);
norm_dist = 1 - ((sim - min(sim)) / (max(sim) - min(sim)));
norm_dist = norm_dist'; % pdist must be 1 X n
Z = linkage(norm_dist);
T = cluster(Z,'cutoff',0.5, 'criterion', 'distance');
dlmwrite('clusters.txt', T);