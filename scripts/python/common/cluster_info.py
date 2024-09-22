class ClusterInfo:
    def __init__(self, env, cluster_type):
        self.env = env
        self.cluster_type = cluster_type

    def get_cluster_type(self) -> str:
        return self.cluster_type

    def get_cluster_env(self) -> str:
        return self.env
