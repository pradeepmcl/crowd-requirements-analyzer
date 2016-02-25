sim = csvread('tf-idf/cosinesim_health.csv', 0, 0);
ids = csvread('tf-idf/reqIds_health.csv', 0, 0);
%norm_dist = 1 - ((sim - min(sim)) / (max(sim) - min(sim)));
norm_dist = 1 - sim;
norm_dist = norm_dist'; % pdist must be 1 X n
Z = linkage(norm_dist);
%T = cluster(Z,'cutoff', 0.7, 'criterion', 'distance');
T = cluster(Z,'cutoff', 1.15);
dlmwrite('tf-idf/clusters.txt', T);

%values = unique(T);
%counts = histc(T(:),values);
%plot(values, sort(instances));