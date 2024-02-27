import Solution
import psycopg2
from Business.Owner import Owner

# conn = psycopg2.connect(
#     database='cs236363',
#     user='dgershko',
#     password='32081020',
#     host='localhost',
# )
# cur = conn.cursor()
# # cur.query("SHOW TABLES;")
# cur.execute("SHOW TABLES;")
# print(cur.fetchall())
# Solution.create_tables()
owner = Owner(1, None)
ret = Solution.add_owner(owner)
print(f"got response from query: ", ret.name)