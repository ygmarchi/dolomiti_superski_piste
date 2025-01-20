import matplotlib.pyplot as plt
import numpy as np

def plot_clusters(X, labels, skied_indices=None, recommand_indices=None, recommand_color='yellow', 
                 recommand_size=100, point_size=50, title='Cluster Visualization',
                 x_label='Feature 1', y_label='Feature 2'):
    """
    Create a scatter plot of clustered data with optional point recommanding.
    
    Parameters:
    -----------
    X : array-like, shape (n_samples, 2)
        The input data to plot (needs to be 2-dimensional)
    labels : array-like, shape (n_samples,)
        Cluster labels for each point
    recommand_indices : list or None
        Indices of points to recommand
    recommand_color : str
        Color to use for recommanded points
    recommand_size : int
        Size of recommanded points
    point_size : int
        Size of regular points
    title : str
        Plot title
    x_label : str
        Label for x-axis
    y_label : str
        Label for y-axis
    """
    # Create the figure and axis
    plt.figure(figsize=(10, 8))
    
    # Get unique clusters
    unique_clusters = np.unique(labels)
    
    # Create scatter plot for each cluster
    for cluster in unique_clusters:
        mask = labels == cluster
        plt.scatter(X[mask, 0], X[mask, 1], 
                   label=f'Cluster {cluster}',
                   alpha=0.6,
                   s=point_size)

    if skied_indices is not None:
        plt.scatter(X[skied_indices, 0], X[skied_indices, 1],
                   facecolor='none',
                   label='skied Points',
                   alpha=0.6,
                   linewidth=1,
                   edgecolor=recommand_color,
                   s=point_size)

    # recommand specific points if provided
    if recommand_indices is not None:
        plt.scatter(X[recommand_indices, 0], X[recommand_indices, 1],
                   color=recommand_color,
                   s=recommand_size,
                   label='recommanded Points',
                   marker='*')
    
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    return plt

def plot (guid):
    # Example usage:
    """
    # Assuming you have your data in X and cluster labels from pgml
    X = your_data  # shape (n_samples, 2)
    labels = your_cluster_labels
    points_to_recommand = [0, 10, 20]  # indices of points you want to recommand

    # Create the plot
    plot = plot_clusters(X, labels, recommand_indices=points_to_recommand)
    plot.show()
    """
    import psycopg2
    conn = psycopg2.connect("host=localhost dbname=postgresml user=postgresml password=postgresml port=5433" )
    try:
        cur = conn.cursor()
        try:
            query = """
                select a.pid 
                    , pgml.decompose('slope_reduction', a.metric_data::vector) coord
                    , pgml.predict('slope_cluster', a.metric_data) cluster 
                from slope_valid a        
            """

            cur.execute(query)
            results = cur.fetchall()

            # Convert to numpy array for visualization
            pids = np.array([row[0] for row in results])
            coords = np.array([row[1] for row in results])
            clusters = np.array([row[2] for row in results])

            query = """
                select distinct d.pid from activity c 
                    join slope_valid d on c.id = d.pid
                    where c.persona_guid = '""" + guid + """'
            """
            cur.execute(query)
            results = cur.fetchall()

            skieds = np.array([row[0] for row in results])
            skied_indices = np.array ([np.where (pids == skied)[0][0]  for skied in skieds])

            query = """
                select get_recommendations_ai_clustering ('""" + guid + """', not_skied_only=> false, usual_region => true)
            """
            cur.execute(query)
            results = cur.fetchall()

            recommendations = np.array([row[0] for row in results])
            recommand_indices = np.array ([np.where (pids == recommendation)[0][0]  for recommendation in recommendations])

            plot = plot_clusters(coords, clusters, skied_indices, recommand_indices)
            plot.show()

        # Close database connection
        finally:
            cur.close()
    finally:
        conn.close()

if __name__ == '__main__':
    import sys
    args = sys.argv
    if (len(args) == 2):
        plot(args[1])        
    else:
        print ('Usage: python plot.py <persona_guid>')