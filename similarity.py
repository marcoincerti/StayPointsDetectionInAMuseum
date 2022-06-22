import psycopg2
import numpy as np
from scipy import spatial
import similaritymeasures

def getPoints(n_person):
    cur = conn.cursor()
    points = []

    cur.execute('select st_x(p.geom), st_y(p.geom) ' \
        'from person_'+ str(n_person) + '_refined as p ' )

    for (x, y) in cur:
        points.append([x,y])

    cur.close()
    return points

def getStayPoints(n_person):
    cur = conn.cursor()
    points = []

    cur.execute('select st_x(p.geom), st_y(p.geom) ' \
        'from stay_points_'+ str(n_person) + ' as p ' )

    for (x, y) in cur:
        points.append([x,y])

    cur.close()
    return points

conn = psycopg2.connect("dbname=GDM_DB user=postgres")

print("Computing similarity on stay points")
points_57 = np.asarray(getStayPoints(57))
points_67 = np.asarray(getStayPoints(67))
points_68 = np.asarray(getStayPoints(68))

dist_57_67 =  similaritymeasures.frechet_dist(points_57, points_67)
print(dist_57_67)
dist_57_68 =  similaritymeasures.frechet_dist(points_57, points_68)
print(dist_57_68)
dist_67_68 =  similaritymeasures.frechet_dist(points_67, points_68)
print(dist_67_68)

print("Computing similarity on trajectories")
points_57 = np.asarray(getPoints(57))
points_67 = np.asarray(getPoints(67))
points_68 = np.asarray(getPoints(68))

dist_57_67 =  similaritymeasures.frechet_dist(points_57, points_67)
print(dist_57_67)
dist_57_68 =  similaritymeasures.frechet_dist(points_57, points_68)
print(dist_57_68)
dist_67_68 =  similaritymeasures.frechet_dist(points_67, points_68)
print(dist_67_68)

