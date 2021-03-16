import json, os, sys
import sqlite3


def convert(instance):
    new_instance = "/".join(instance.split("."))
    return "/p/css03/esgf_publish/" + new_instance + "/"


if __name__ == "__main__":
    
    fn = '/p/user_pub/xclim/persist/xml.db'
    conn = sqlite3.connect(fn)
    conn.row_factory = sqlite3.Row
    cr = conn.execute("select path from paths where mip_era='CMIP6' and retired = 0;")
    res = list(cr.fetchall())
    print(len(res))
    hr_list = []
    sql_list = res
    #sql_list.sort()
    print(sql_list[0][0])
    with open("have_replicas.txt") as hr:
        for line in hr:
            hr_list.append(convert(line))
    #hr_list.sort()

    new_f = open("sql_results.txt", "w")
    for r in sql_list:
        for path in r:
            if path not in hr_list:
                new_f.write(path + "\n") 
    new_f.close()
