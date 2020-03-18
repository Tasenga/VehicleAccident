from create_table import connect_postgresql


def check_intersection_of_geolocations(connect):
    cur = connect.cursor()
    cur.execute('''UPDATE accidents SET borough = boroughs.id FROM boroughs
  WHERE accidents.borough is NULL AND ST_INTERSECTS(accidents.location, boroughs.geom)
  RETURNING accidents.id, accidents.borough, boroughs.id, boroughs.borough;''')
    print("Records about borough were updated successfully")
    connect.commit()

if __name__ == '__main__':
    con = connect_postgresql()
    check_intersection_of_geolocations(con)


# Messages
# Succesfully run. Total query runtime: 5 min 11 secs.
# 202842 rows affected.
# 652 str/sec