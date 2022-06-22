import psycopg2

class Point:
    def __init__(self, id, arrivetime, leavetime):
        self.id = id
        self.arrivetime = arrivetime
        self.leavetime = leavetime


def resetInterestingExhibits():
    print("RESETTING PREVIOUS RESULTS")
    cur = conn.cursor()


    people = [57, 67, 68]
    for p in people:
        cur.execute("drop table if exists exhibits_person_" + str(p))


    conn.commit()
    cur.close()

def interestingExhibits(n_person):
    print("FINDING INTERESTING EXHIBITS FOR PERSON ", n_person)
    cur = conn.cursor()

    cur.execute('create table if not exists exhibits_person_'+ str(n_person) +  ''' as (
        SELECT p.id as pointId, e.id as exhibitId, p.geom as point, p.arrivtime, p.leavetime, e.geom as geom
        FROM exhibits_on_tables as e, (
            select p.geom, min(st_distance(p.geom, e.geom))as min ''' + 
            'from stay_points_'+ str(n_person) +  ''' as p, exhibits_on_tables as e
            group by p.geom ''' + 
            ') as i JOIN stay_points_'+ str(n_person) +  ''' as p ON p.geom = i.geom
        where st_distance(p.geom, e.geom) = i.min 
        and st_distance(p.geom, e.geom) < 1)''')

    conn.commit()
    cur.close()

# Connect to your postgres DB
print("Connecting to database...")
conn = psycopg2.connect("dbname=GDMD user=postgres password=khliabub")
resetInterestingExhibits()

people = [57, 67, 68]
for p in people:
    interestingExhibits(p)
