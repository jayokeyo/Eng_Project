'''The DFA server process controls the entire frequency allocation process.
Client processes invoke the server process for rainrate data acquisition,
allocation status check, frequency allocation and deallocation, etc.
The DFA_Server has an active connection with MySQL database that hosts the
data that drives the system. The functions are named in a descriptive manner
to make it easy to understand what they are intended to do!'''

global _Footprint_ID
global _Rainrate
global _Footprint_Neighbors
global _Privileged
global _Allocated_Frequency
_Privileged = {}

def Initialize_Footprints():
    global _Footprint_ID
    global _Footprint_Neighbors
    _Footprint_ID = {}
    _Footprint_Neighbors = {}
    x = 19
    import mysql.connector as mysql
    db = mysql.connect(
        host = "localhost",
        user = "****",
        passwd = "****",
        database = "DFAdb"
    )
    cursor = db.cursor()
    cursor.execute("SELECT * FROM Footprint_IDs")
    data = cursor.fetchall()
    Row = data[0]
    for i in range (1,x+1):
        _Footprint_ID[str('Footprint_'+str(i))] = Row[i-1]
    for value in _Footprint_ID.values():
        cursor.execute("SELECT * FROM "+str(value)+"_Neighbors")
        data = cursor.fetchall()
        data = data[0]
        for n in range (1,7):
            _Footprint_Neighbors[str(value)+'_Neighbor_'+str(n)] = data[n-1]

def Fetch_Rainrate(Footprint_ID):
    global _Rainrate
    _Rainrate = {}
    import mysql.connector as mysql
    db = mysql.connect(
            host = "localhost",
            user = "*****",
            passwd = "*****",
            database = "DFAdb"
        )
    for value in Footprint_ID.values():
        cursor = db.cursor()
        cursor.execute("SELECT Rainrate FROM "+str(value))
        Record = cursor.fetchall()
        Tuple = Record[0]
        _Rainrate[value+str('_Rainrate')] = Tuple[0]
    return _Rainrate

def Frequency(Footprint_ID):
    global _Frequency
    _Frequency = {}
    import mysql.connector as mysql
    db = mysql.connect(
        host = "localhost",
        user = "*****",
        passwd = "*****",
        database = "DFAdb"
    )
    cursor = db.cursor()
    query1 = "SELECT Frequency FROM Regression_Coefficients"
    cursor.execute(query1)
    data1 = cursor.fetchall()
    i = 1
    for value in data1:
        _Frequency[str('Frequency')+str(i)] = value[0]
        i += 1
    return _Frequency

def Calculate_Specific_Attenuation(Footprint_ID,Footprint_Theta,Footprint_Tao):
    global _Specific_Attenuation
    from math import sqrt, cos, atan, sin, exp
    _Specific_Attenuation = {}
    import mysql.connector as mysql
    db = mysql.connect(
        host = "localhost",
        user = "****",
        passwd = "*****",
        database = "DFAdb"
    )
    cursor = db.cursor()
    query2 = "SELECT kH,kV,aH,aV FROM Regression_Coefficients"
    cursor.execute(query2)
    data2 = cursor.fetchall()
    ID = str(Footprint_ID)
    i = 1
    for kH,kV,aH,aV in data2:
        Frequency = _Frequency[str('Frequency')+str(i)]
        k = float((kH+kV+((kH-kV)*(pow((cos(Footprint_Theta)),2)*(cos(2*(Footprint_Tao))))))/2)
        a = float ((kH*aH+kV*aV+((kH*aH-kV*aV)*(pow((cos(Footprint_Theta)),2)*(cos(2*(Footprint_Tao))))))/(2*k))
        R = _Rainrate[ID+'_Rainrate']
        _Specific_Attenuation[ID+str('_')+str(Frequency)+str('_Hz')+str('_S_Attenuation')] = (k*(pow(R,a)))
        i += 1
    return _Specific_Attenuation

def Update_Privileged_Dictionary(_Satellite_ID,Footprint_ID,Frequency,a):
    global _Privileged
    _Privileged[_Satellite_ID+str('_')+Footprint_ID+str('_Channel_')+str(a)] = Frequency

def Allocate_Frequencies(Satellite_ID,Footprint_ID,Threshold_Frequency,Number_of_Channels,Previously_Priviledged):
    global _Privileged
    global Allocated_Frequency
    Allocated_Frequency = {}
    _Available_Bands_Locally = {}
    _Available_Bands_Neighbor_1 = {}
    _Available_Bands_Neighbor_2 = {}
    _Available_Bands_Neighbor_3 = {}
    _Available_Bands_Neighbor_4 = {}
    _Available_Bands_Neighbor_5 = {}
    _Available_Bands_Neighbor_6 = {}
    _Available_Bands_Privileged = {}
    if Previously_Priviledged == True:
        print(_Privileged)
        for a in range (0,Number_of_Channels):
            alpha = {}
            alpha.add(Satellite_ID+'_'+Footprint_ID+'_Channel_'+str(a+1))
        beta = set(_Privileged)
        _Privileged = alpha - beta
        print(_Privileged)
    import mysql.connector as mysql
    db = mysql.connect(
        host = "localhost",
        user = "****",
        passwd = "****",
        database = "DFAdb"
    )
    cursor = db.cursor()
    query = "SELECT Frequency_Band FROM "+Footprint_ID+" WHERE Allocation_Status = 'Unallocated'"
    cursor.execute(query)
    data = cursor.fetchall()
    print(data)
    for value in data:
        value = value[0]
        if value < Threshold_Frequency:
            _Available_Bands_Locally[Footprint_ID+str('_')+str(value)+str('_Available_Locally')] = value
    cursor = db.cursor()
    cursor.execute("SELECT * FROM "+Footprint_ID+"_Neighbors")
    data = cursor.fetchall()
    data = data[0]
    print(data)
    cursor = db.cursor()
    Table = data[0]
    query = "SELECT Frequency_Band FROM "+Table+" WHERE Allocation_Status = 'Unallocated'"
    cursor.execute(query)
    data2 = cursor.fetchall()
    for value in data2:
        value = value[0]
        if value < Threshold_Frequency:
            _Available_Bands_Neighbor_1[Table+str('_')+str(value)+str('_Available_Neighbor_1')] = value
    Table = data[1]
    cursor = db.cursor()
    query = "SELECT Frequency_Band FROM "+str(Table)+" WHERE Allocation_Status = 'Unallocated'"
    cursor.execute(query)
    data2 = cursor.fetchall()
    for value in data2:
        value = value[0]
        if value < Threshold_Frequency:
            _Available_Bands_Neighbor_2[Table+str('_')+str(value)+str('_Available_Neighbor_2')] = value
    Table = data[2]
    cursor = db.cursor()
    query = "SELECT Frequency_Band FROM "+str(Table)+" WHERE Allocation_Status = 'Unallocated'"
    cursor.execute(query)
    data2 = cursor.fetchall()
    for value in data2:
        value = value[0]
        if value < Threshold_Frequency:
            _Available_Bands_Neighbor_3[Table+'_'+str(value)+str('_Available_Neighbor_3')] = value
    Table = data[3]
    cursor = db.cursor()
    query = "SELECT Frequency_Band FROM "+str(Table)+" WHERE Allocation_Status = 'Unallocated'"
    cursor.execute(query)
    data2 = cursor.fetchall()
    for value in data2:
        value = value[0]
        if value < Threshold_Frequency:
            _Available_Bands_Neighbor_4[Table+'_'+str(value)+str('_Available_Neighbor_4')] = value
    Table = data[4]
    cursor = db.cursor()
    query = "SELECT Frequency_Band FROM "+str(Table)+" WHERE Allocation_Status = 'Unallocated'"
    cursor.execute(query)
    data2 = cursor.fetchall()
    for value in data2:
        value = value[0]
        if value < Threshold_Frequency:
            _Available_Bands_Neighbor_5[Table+'_'+str(value)+str('_Available_Neighbor_5')] = value
    Table = data[5]
    cursor = db.cursor()
    query = "SELECT Frequency_Band FROM "+str(Table)+" WHERE Allocation_Status = 'Unallocated'"
    cursor.execute(query)
    data2 = cursor.fetchall()
    for value in data2:
        value = value[0]
        if value < Threshold_Frequency:
            _Available_Bands_Neighbor_6[Table+str('_')+str(value)+str('_Available_Neighbor_6')] = value
    a = set(_Available_Bands_Locally.values())
    b = set(_Available_Bands_Neighbor_1.values())
    c = set(_Available_Bands_Neighbor_2.values())
    d = set(_Available_Bands_Neighbor_3.values())
    e = set(_Available_Bands_Neighbor_4.values())
    f = set(_Available_Bands_Neighbor_5.values())
    g = set(_Available_Bands_Neighbor_6.values())
    Available_Bands = set.intersection(a,b,c,d,e,f,g)
    print(Available_Bands)
    if len(Available_Bands)>=Number_of_Channels:
        for a in range (0,Number_of_Channels):
            Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)] = max(Available_Bands)
            Available_Bands.remove(max(Available_Bands))
            cursor = db.cursor() 
            cursor.execute("UPDATE "+Footprint_ID+" SET Allocation_Status= 'Active' WHERE Frequency_Band = "+str(Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)]))
            db.commit()
            cursor.execute("UPDATE "+Footprint_ID+" SET Satellite_ID= '"+Satellite_ID+"' WHERE Frequency_Band= "+str(Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)]))
            db.commit()
            cursor.execute("UPDATE "+Footprint_ID+" SET Channel_ID= "+str(a+1)+" WHERE Frequency_Band = "+str(Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)]))
            db.commit()
            if (Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)] - Threshold_Frequency) > 0.5:
                Update_Privileged_Dictionary(Satellite_ID,Footprint_ID,Threshold_Frequency,(a+1))
        print(Allocated_Frequency)
    else:
        i = 1
        for value in _Privileged.values():
            if value < Threshold_Frequency:
                Deallocate_Frequency(value,Footprint_ID)
                a.add(value)
                b.add(value)
                c.add(value)
                d.add(value)
                e.add(value)
                f.add(value)
                g.add(value)
                Available_Bands = set.intersection(a,b,c,d,e,f,g)
                if len(Available_Bands)>=Number_of_Channels:
                    for a in range (0,Number_of_Channels):
                        Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)] = max(Available_Bands)
                        Available_Bands.remove(max(Available_Bands))
                        cursor = db.cursor()
                        cursor.execute("UPDATE "+Table+" SET Allocation_Status = 'Active' WHERE Frequency_Bands = "+str(Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)]))
                        db.commit()
                        cursor.execute("UPDATE "+Footprint_ID+" SET Satellite_ID = '"+str(Satellite_ID)+"' WHERE Frequency_Bands = "+str(Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)]))
                        db.commit()
                        cursor.execute("UPDATE "+Footprint_ID+"SET Channel_ID = "+str(a + 1)+" WHERE Frequency_Bands = "+str(Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)]))
                        db.commit()
                        if (Allocated_Frequency[Satellite_ID+'_'+Footprint_ID+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)] - Threshold_Frequency) > 0.5:
                            Update_Privileged_Dictionary(Satellite_ID,Footprint_ID,Threshold_Frequency,(a + 1))
                    break
    return Allocated_Frequency

def Deallocate_Frequency(Frequency,Footprint_ID):
    import mysql.connector as mysql
    db = mysql.connect(
        host = "localhost",
        user = "********",
        passwd = "**********",
        database = "DFAdb"
    )
    cursor = db.cursor()
    query = "UPDATE "+Footprint_ID+" SET Allocation_Status = 'Unallocated',Satellite_ID = 'NULL', Channel_ID = 'NULL' WHERE Frequency_Band = "+str(Frequency)
    cursor.execute(query)
    db.commit()

def Allocation_Status(Satellite_ID,Footprint_ID):
    import mysql.connector as mysql
    db = mysql.connect(
            host = "localhost",
            user = "******",
            passwd = "******",
            database = "DFAdb"
        )
    for value in Footprint_ID.values():
        cursor = db.cursor()
        cursor.execute("SELECT Channel_ID FROM "+value+" WHERE Satellite_ID = '"+Satellite_ID+"'")
        Record = cursor.fetchall()
    return Record
