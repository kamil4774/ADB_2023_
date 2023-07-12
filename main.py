from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer,  MetaData, ForeignKey, Table, Sequence, Text, Double
from sqlalchemy import or_, and_
from sqlalchemy.sql import text
from sqlalchemy import select
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Required to cast results of the simulation from string to the list of results
def string_to_array(input_string):
    while(input_string[0] == ' '):
        if(input_string[0] == ' '):
            input_string = input_string[1:]
    counter = 0
    list_of_elems = input_string.replace(' ', ',')
    list_of_elems = list_of_elems.split(',')

    list_of_elems = [x for x in list_of_elems if x != '']

    for elem in list_of_elems:
        counter = counter + 1
        if(elem == ''):
            print(counter)
            print(len(list_of_elems))

    list_of_elems_f = [float(x) for x in list_of_elems]
    return np.array(list_of_elems_f)


db_string = "postgresql://engineer1:1234@localhost:5432/new_database"

engine = create_engine(db_string)

base = declarative_base()
meta = MetaData

# definition of the class representation
class K_param(base):
    __tablename__ = "k_param"
    id = Column(Integer, Sequence("k_param_id"), primary_key=True)
    unit = Column("unit", String(10))
    value = Column("value", Double)
    results = relationship("Results", back_populates="k_param")

    def __init__(self, value, unit):
        self.value = value
        self.unit = unit


class L_param(base):
    __tablename__ = "l_param"
    id = Column(Integer, Sequence("l_param_id"), primary_key=True)
    unit = Column("unit", String(10))
    value = Column("value", Double)
    results = relationship("Results", back_populates="l_param")

    def __init__(self, value, unit):
        self.value = value
        self.unit = unit

class M_param(base):
    __tablename__ = "m_param"
    id = Column(Integer, Sequence("m_param_id"), primary_key=True)
    unit = Column("unit", String(10))
    value = Column("value", Double)
    results = relationship("Results", back_populates="m_param")

    def __init__(self, value, unit):
        self.value = value
        self.unit = unit

class D_param(base):
    __tablename__ = "d_param"
    id = Column(Integer, Sequence("d_param_id"), primary_key=True)
    unit = Column("unit", String(10))
    value = Column("value", Double)
    results = relationship("Results", back_populates="d_param")

    def __init__(self, value, unit):
        self.value = value
        self.unit = unit


class Results(base):
    __tablename__ = "results"
    id      = Column(Integer, Sequence("result_id"), primary_key=True)

    k_id    = Column(Integer, ForeignKey("k_param.id"))
    l_id    = Column(Integer, ForeignKey("l_param.id"))
    d_id    = Column(Integer, ForeignKey("d_param.id"))
    m_id    = Column(Integer, ForeignKey("m_param.id"))

    k_param = relationship("K_param", back_populates="results")
    l_param = relationship("L_param", back_populates="results")
    d_param = relationship("D_param", back_populates="results")
    m_param = relationship("M_param", back_populates="results")

    simulation_real_id = Column("simulation_real_id", Integer)

    rot_x   = Column("rot_x", Text)
    rot_y   = Column("rot_y", Text)
    rot_z   = Column("rot_z", Text)

    t_vec   = Column("t_vec", Text)

    def __init__(self, rot_x, rot_y, rot_z, t_vec):
        self.rot_x = rot_x
        self.rot_y = rot_y
        self.rot_z = rot_z

        self.t_vec = t_vec

#creating a session and connect to database
conn = engine.connect()

#delete database - for testing purposes, later it wouldn't be used

trans = conn.begin()

base.metadata.drop_all(engine)

trans.commit()

base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


# read data from csv
# Data format:
# ID:int, K:int, L:int, M:int, D:int, Rot_x:string, Rot_y:string, Rot_z:string, T_vec:string
# ID - id of simulation
# K - Spring coefficient
# L - natural length of spring
# M - mass of the weight
# D - damping coefficient
# Rot_x - vector of rotation of sphere around x axis
# Rot_y - vector of rotation of sphere around y axis
# Rot_z - vector of rotation of sphere around z axis
# t_vec - time vector of simulation

results_df = pd.read_csv("simulation_result.csv", nrows=30)
results_df.columns = results_df.columns.str.replace(" ", "")

results_df.fillna("")

results_df = results_df.rename(columns= {"k" : "k_param", "l" : "l_param", "m" : "m_param", "d" : "d_param", "id" : "simulation_real_id"})

# K-param table
k_list = pd.DataFrame(results_df["k_param"]).drop_duplicates().reset_index().drop(columns = ["index"])
k_list = k_list.rename(columns = {"k_param" : "value"})

unit_list = ["N/m"]*k_list.shape[0]
k_list["unit"] = unit_list
k_list.index.name = "id"

# L-param table
l_list = pd.DataFrame(results_df["l_param"]).drop_duplicates().reset_index().drop(columns = ["index"])
l_list = l_list.rename(columns = {"l_param" : "value"})

unit_list = ["mm"]*l_list.shape[0]
l_list["unit"] = unit_list
l_list.index.name = "id"

# M-param table
m_list = pd.DataFrame(results_df["m_param"]).drop_duplicates().reset_index().drop(columns = ["index"])
m_list = m_list.rename(columns = {"m_param" : "value"})

unit_list = ["g"]*m_list.shape[0]
m_list["unit"] = unit_list
m_list.index.name = "id"

# D-param table
d_list = pd.DataFrame(results_df["d_param"]).drop_duplicates().reset_index().drop(columns = ["index"])
d_list = d_list.rename(columns = {"d_param" : "value"})

d_list["value"] = d_list["value"].astype(float)[0]

unit_list = ["Ns^2/m"]*d_list.shape[0]
d_list["unit"] = unit_list
d_list.index.name = "id"


list_of_points_z = string_to_array(results_df['rot_z'][0])
list_of_points_t = string_to_array(results_df['t_vec'][0])

# results table
results_list = results_df.rename(columns= {"k_param" : "k_id", "l_param" : "l_id", "m_param" : "m_id", "d_param" : "d_id"})

temp_list = ["k_id", "l_id", "m_id", "d_id"]
handle_list = [k_list, l_list, m_list, d_list]

for i in range(len(temp_list)):
    col = temp_list[i]
    handle = handle_list[i]
    temp_string = "value"
    results_list[col] = results_list[col].map(lambda x: handle[handle[temp_string] == x].index.values.astype(int)[0])

results_list.index.name = "id"


handle_list.append(results_list)

table_names_list = ["k_param", "l_param", "m_param", "d_param", "results"]

for i in range(len(handle_list)):
    handle = handle_list[i]
    table_name = table_names_list[i]
    handle.to_sql(table_name, engine, if_exists="append")


tables_dict = {}
for table_name in base.metadata.tables.keys():

    tables_dict[table_name] = Table(table_name, base.metadata, autoload=True, autoload_with=engine)

if __name__ == '__main__':
    print("info about k_param table")
    print(repr(tables_dict["k_param"]))
    print(tables_dict["k_param"].columns.keys())

    # Example queries
    # Query using plain text
    stmt = text("""select * from m_param""")
    results_stmt = conn.execute(stmt).fetchall()
    print("Result of the query select * from m_param")
    print(results_stmt)
    print("Type of the result:")
    print(type(results_stmt))

    print("Query with session.query, rows:")
    result_query = session.query(M_param)

    for row in result_query:
        print("Id ", row.id, "Value", row.value, "Unit", row.unit)
    print("Query with session.query, column description:")
    print(result_query.column_descriptions)
    print("Query with session.query, select:")
    print(result_query)

    print("Query using select statement - mapper for all table:")
    stmt = select(tables_dict['l_param'])
    print("Used statement:")
    print(stmt)
    print("Returned table:")
    results_stmt = conn.execute(stmt).fetchall()

    print(results_stmt)

    print("Query using select statement - mapper for columns table:")
    stmt = select(tables_dict['l_param'].c.value, tables_dict['l_param'].c.id)
    print("Used statement:")
    print(stmt)
    print("Returned table:")
    results_stmt = conn.execute(stmt).fetchall()

    print(results_stmt)

    # Filter results by the k, and l

    print("Query with filter using session.query, filter l == 15:")

    filter_query = (session.query(Results.simulation_real_id, L_param.id, L_param.value).join(
        Results)\
        .join(M_param)\
        .filter(
        L_param.value == 15))

    print("Used statement:")
    print(filter_query)

    results_filter_query = filter_query.all()
    print("result statement:")
    print(results_filter_query)

    print("Query with filter using session.query, filter m == 50:")
    filter_query = session.query(Results.simulation_real_id, M_param.id, M_param.value).join(
        Results).filter(
        M_param.value == 50)

    print(filter_query)
    print("Used statement:")
    results_filter_query = filter_query.all()
    print("result statement:")
    print(results_filter_query)

    print("Query returning multiple parameters and with two conditions, l == 15 and m == 50")
    text_stmt = text("""select results.simulation_real_id, l_param.value, m_param.value, k_param.value  from results\n""" +
                      """inner join m_param\n""" +
                      """on m_param.id = results.m_id\n""" +
                      """inner join l_param\n""" +
                      """on l_param.id = results.l_id\n""" +
                      """inner join k_param\n""" +
                      """on k_param.id = results.k_id\n""" +
                      """where l_param.value = 15 and m_param.value = 50;"""
                     )
    print("Text of query:")
    print(text_stmt)

    results_stmt = conn.execute(text_stmt).fetchall()

    print("Result list")
    print(results_stmt)
    for elem in results_stmt:
        print(elem)

## Plotting results from the simulation in the z axis
    print("Example of ploting results from database:")
    text_stmt = text("""select results.t_vec, results.rot_z, l_param.value, m_param.value, k_param.value  from results\n""" +
                      """inner join m_param\n""" +
                      """on m_param.id = results.m_id\n""" +
                      """inner join l_param\n""" +
                      """on l_param.id = results.l_id\n""" +
                      """inner join k_param\n""" +
                      """on k_param.id = results.k_id\n""" +
                      """where l_param.value = 15 and m_param.value = 50;"""
                     )
    print(text_stmt)

    results_stmt = conn.execute(text_stmt).fetchall()
    lines = []
    for result_elem in results_stmt:
        time_vec = string_to_array(result_elem[0])
        rot_z = string_to_array(result_elem[1])
        l_value = result_elem[2]
        m_value = result_elem[3]
        k_value = result_elem[4]
        label_to_write = "k = " + str(k_value) + " l = " + str(l_value) + " m_value = " + str(m_value)

        line = plt.plot(time_vec, rot_z, label=label_to_write)
        plt.xlabel("Time")
        plt.ylabel("Rotation(rad/s)")
        lines.append(label_to_write)
    plt.legend(lines)
    plt.show()

#


