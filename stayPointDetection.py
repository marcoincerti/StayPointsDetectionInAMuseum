
import psycopg2
import math
import time
from datetime import datetime

class Point:
    def __init__(self, id, x, y, timestamp):
        self.id = id
        self.x = x
        self.y = y
        self.timestamp = timestamp

class stayPoint():
    def __init__(self, id, x, y, arrivTime, leaveTime):
        self.id = id
        self.x = x
        self.y = y
        self.arrivTime = arrivTime
        self.leaveTime = leaveTime

    def __str__(self):
        return str(self.x) + " " + str(self.y) + " " + str(self.arrivTime) + " " + str(self.leaveTime)

def resetStayPoints():
    print("RESETTING PREVIOUS RESULTS")
    cur = conn.cursor()

    people = [57, 67, 68]
    for p in people:
        cur.execute("drop table if exists stay_points_" + str(p))
        cur.execute("drop table if exists intersection_tables_" + str(p))
        cur.execute("drop table if exists closest_points_" + str(p))
        cur.execute("drop table if exists person_" + str(p) + "_refined")


    conn.commit()
    cur.close()


def preprocessing(): 
    print("PRE-PROCESSING DATASET")
    cur = conn.cursor()

    # On person 57
    cur.execute('create table if not exists intersection_tables_57 as (select distinct p1.id, p1.geom as geom '\
        'from person_57 as p1 ' \
        'where p1.geom in (select distinct(st_intersection(p1.geom, t.geom)) as geom ' \
        'from person_57 as p1, tables as t))')

    cur.execute('create table if not exists closest_points_57 as (select distinct p.id, st_closestpoint(st_boundary(t.geom), p.geom) as geom ' \
                'from intersection_tables_57 as p, tables as t '\
                'where st_intersects(p.geom, t.geom))')

    cur.execute('create table if not exists person_57_refined as (select * from person_57)')

    cur.execute('UPDATE person_57_refined '\
        'SET    geom = c.geom ' \
        'FROM   closest_points_57 as c '\
        'WHERE  c.id = person_57_refined.id')

    # On person 67
    cur.execute('create table if not exists intersection_tables_67 as (select distinct p1.id, p1.geom as geom '\
        'from person_67 as p1 ' \
        'where p1.geom in (select distinct(st_intersection(p1.geom, t.geom)) as geom ' \
        'from person_67 as p1, tables as t))')

    cur.execute('create table if not exists closest_points_67 as (select distinct p.id, st_closestpoint(st_boundary(t.geom), p.geom) as geom ' \
                'from intersection_tables_67 as p, tables as t '\
                'where st_intersects(p.geom, t.geom))')

    cur.execute('create table if not exists person_67_refined as (select * from person_67)')

    cur.execute('UPDATE person_67_refined '\
        'SET    geom = c.geom ' \
        'FROM   closest_points_67 as c '\
        'WHERE  c.id = person_67_refined.id')

    # On person 68
    cur.execute('create table if not exists intersection_tables_68 as (select distinct p1.id, p1.geom as geom '\
        'from person_68 as p1 ' \
        'where p1.geom in (select distinct(st_intersection(p1.geom, t.geom)) as geom ' \
        'from person_68 as p1, tables as t))')

    cur.execute('create table if not exists closest_points_68 as (select distinct p.id, st_closestpoint(st_boundary(t.geom), p.geom) as geom ' \
                'from intersection_tables_68 as p, tables as t '\
                'where st_intersects(p.geom, t.geom))')

    cur.execute('create table if not exists person_68_refined as (select * from person_68)')

    cur.execute('UPDATE person_68_refined '\
        'SET    geom = c.geom ' \
        'FROM   closest_points_68 as c '\
        'WHERE  c.id = person_68_refined.id')

    conn.commit()
    cur.close()


def getPoints(n_person):
    cur = conn.cursor()
    points = []

    cur.execute('select p.id, st_x(p.geom), st_y(p.geom), timestamp ' \
        'from person_'+ str(n_person) + '_refined as p ' )

    for (id, x, y, timestamp) in cur:
        points.append(Point(id,x,y,timestamp))

    cur.close()
    return points

def computMeanCoord(candidatePoints, i, j):
    x = 0.0
    y = 0.0
    if j-i+1 == 0:
        return float(candidatePoints[i].x), float(candidatePoints[i].y)
    for point in candidatePoints:
        x += float(point.x)
        y += float(point.y)
    return float(x/len(candidatePoints)), float(y/len(candidatePoints))



def stayPointDetectionAlgorithm(ptraj, distThres = 1, timeThres = 30):
    print("EXECUTING STAY POINT DETECTION ALGORITHM WITH distThres = {} m AND timeThresh = {} s".format(distThres, timeThres))
    stayPointList = []
    points = ptraj
    pointNum = len(points)
    id = 1
    i = 0
    while i < pointNum: 
        j = i+1
        token = 0
        while j < pointNum:
            dist = math.dist([points[i].x, points[i].y], [points[j].x, points[j].y])

            if dist > float(distThres):
                t_i = time.mktime(datetime.strptime(points[i].timestamp[:18], "%Y/%m/%d %H:%M:%S").timetuple())
                t_j = time.mktime(datetime.strptime(points[j].timestamp[:18], "%Y/%m/%d %H:%M:%S").timetuple())
                deltaT = t_j - t_i
                if deltaT > timeThres:
                    tmp_x, tmp_y = computMeanCoord(points[i:j], i, j-1)
                    tmp_arrivTime, tmp_leaveTime = int(t_i), int(t_j)
                    sp = stayPoint(id, tmp_x,tmp_y,time.strftime("%H:%M:%S", time.localtime(tmp_arrivTime)),time.strftime("%H:%M:%S", time.localtime(tmp_leaveTime)))
                    stayPointList.append(sp)
                    id += 1
                    i = j
                    token = 1
                break
            j += 1
        if token != 1:
            i += 1
    return stayPointList

def saveStayPoints(n_person, stay_points):
    print("SAVING STAY POINTS IN THE DATABASE")
    cur = conn.cursor()

    cur.execute('create table if not exists stay_points_'+ str(n_person) + ' ('\
        'id integer, '\
        'geom geometry, ' \
        'arrivTime time, '\
        'leaveTime time )' )

    #sql_insert_query = 'INSERT INTO stay_points_'+ str(n_person) + ' '\
    #    '(geom, arrivTime, leaveTime, duration) VALUES (ST_Point(%s,%s), %s, %s, %s)'

    for p in stay_points:
        cur.execute('INSERT INTO stay_points_'+ str(n_person) + ' '\
        '(id, geom, arrivTime, leaveTime) VALUES (%(id)s, ST_SetSRID(ST_Point(%(x)s,%(y)s)::geometry, 3003), %(arrivTime)s, %(leaveTime)s)',
        {'id': p.id, 'x': p.x, 'y': p.y, 'arrivTime': p.arrivTime, 'leaveTime': p.leaveTime})

    #cur.executemany(sql_insert_query, [stay_points])
    conn.commit()
    cur.close()


# Connect to your postgres DB
print("Connecting to database...")
conn = psycopg2.connect("dbname=GDMD user=postgres")
resetStayPoints()

preprocessing()

people = [57, 67, 68]
for p in people:
    points = getPoints(p)
    stay_points = stayPointDetectionAlgorithm(points)
    saveStayPoints(p, stay_points)

conn.close()
