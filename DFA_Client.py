import rpyc
from math import cos, sin
from time import sleep

conn001 = rpyc.classic.connect("127.0.0.1")
conn001.execute("import DFA_Server")

global Satellite_Longitude
global Satellite_Latitude
global _Footprint_ID
global _Footprint_Latitude
global _Footprint_Theta
global _Footprint_Tao
global _Footprint_Altitude
global _Rain_Height
global _Slant_Path
global _Horizontal_Projection
global _Rainrate
global _Current_Rainrate
global _Threshold_Frequency
global _Number_of_Channels
global _Threshold_Attenuation
global _Specific_Attenuation
global Allocated_Frequency
global Previously_Priviledged

Satellite_ID = str(input('Satellite identity: '))

def Initialize_Satellite ():
    global _Footprint_ID
    global _Footprint_Latitude
    global _Footprint_Theta
    global _Footprint_Altitude
    global _Number_of_Channels
    global _Threshold_Attenuation
    global _Footprint_Tao
    global Satellite_Longitude
    global Satellite_Latitude
    Satellite_Longitude = int(input('Longitude of the satellite in degrees: '))
    Satellite_Latitude = int(input('Latitude of the satellite in degrees: '))
    _Number_of_Footprints = int(input('Number of footprints transmitting to: '))
    _Footprint_ID = {}
    _Footprint_Latitude = {}
    _Footprint_Theta = {}
    _Footprint_Tao = {}
    _Footprint_Altitude = {}
    _Number_of_Channels = {}
    _Threshold_Attenuation = {}
    for x in range(0,_Number_of_Footprints):
        Footprint_ID = str(input ('Unique ID of the footprint: '))
        _Footprint_ID['Footprint_'+str(x)] = Footprint_ID
        Footprint_Theta = float(input('Angle of elevation between the farthest location within the footprint\
and the satellite(Angle between 0 and 90): '))
        _Footprint_Theta[Footprint_ID+str('_Theta')] = Footprint_Theta
        Footprint_Tao = float(input('Angle of radiowave polarization (Horizontal polarization = 0,\
Circular polarization = 45, Vertical polarization = 90): '))
        _Footprint_Tao[Footprint_ID+str('_Tao')] = Footprint_Tao
        Footprint_Altitude = float(input('Altitude of the farthest location within the footprint in Km '))
        _Footprint_Altitude[Footprint_ID+str('_Altitude')] = Footprint_Altitude
        Footprint_Latitude = int(input('Latitude of the farthest location within the footprint\
(Key in Latitudes within the southern hemisphere as negative values and\
latitudes within the northern hemisphere as positive values): '))
        _Footprint_Latitude[Footprint_ID+str('_Latitude')] = Footprint_Latitude
        Number_of_Channels = int(input('Number of channels in the footprint: '))
        _Number_of_Channels[Footprint_ID+str('_Channels')] = Number_of_Channels
        Threshold_Attenuation = int(input('Attenuation that must not be exceeded within the footprint: '))
        _Threshold_Attenuation[Footprint_ID+str('_Th_Attenuation')] = Threshold_Attenuation

def Calculate_Rain_Height():
    global _Rain_Height
    _Rain_Height = {}
    for value in _Footprint_ID.values():
        if _Footprint_Latitude[value+str('_Latitude')] > 23:
            _Rain_Height[value + str('_Rain_Height')] = 5 - 0.075*(_Footprint_Latitude[value+str('_Latitude')] - 23)
        if 23 >= _Footprint_Latitude[value+str('_Latitude')] >= 0:
            _Rain_Height[value + str('_Rain_Height')] = 5
        if -21 <= _Footprint_Latitude[value+str('_Latitude')] < 0:
            _Rain_Height[value + str('_Rain_Height')] = 5
        if -71 <= _Footprint_Latitude[value+str('_Latitude')] < -21:
            _Rain_Height[value + str('_Rain_Height')] = 5 + 0.1* (_Footprint_Latitude[value+str('_Latitude')] + 21)
        if -71 > _Footprint_Latitude[value+str('_Latitude')]:
                _Rain_Height[value + str('_Rain_Height')] = 0

def Calculate_Slant_Path():
    global _Slant_Path
    R = float(8500)
    _Slant_Path = {}
    for value in _Footprint_ID.values():
        if _Footprint_Theta[value+str('_Theta')] > 5:
            Ls = (_Rain_Height[value+str('_Rain_Height')]-_Footprint_Altitude[value+str('_Altitude')])/(sin(_Footprint_Theta[value+str('_Theta')]))
            _Slant_Path[value+str('_Ls')] = abs(Ls)
        else:
            Ls = (2.0*(_Rain_Height[value+str('_Rain_Height')]-_Footprint_Altitude[value+str('_Altitude')]))/((pow((sin(_Footprint_Theta[value+str('_Theta')])),2.0))+(pow(((2.0*(_Rain_Height[value+str('_Rain_Height')]-_Footprint_Altitude[value+str('_Altitude')]))/R),0.5))+sin(_Footprint_Theta[value+str('_Theta')]))
            _Slant_Path[value+str('_Ls')] = abs(Ls)

def Calculate_Horizontal_Projection():
    global _Horizontal_Projection
    _Horizontal_Projection ={}
    for value in _Footprint_ID.values():
        Horizontal_Projection = _Slant_Path[value+str('_Ls')]*cos(_Footprint_Theta[value+str('_Theta')])
        _Horizontal_Projection[value+str('_Lg')] = abs(Horizontal_Projection)

def Check_Allocation_Status(Satellite_ID,Footprint_ID):
    Record_Dictionary = {}
    a = set(range(1,(_Number_of_Channels[Footprint_ID+str('_Channels')]+1)))
    print(a)
    Record = conn001.modules.IBM_Edit_Server.Allocation_Status(Satellite_ID,_Footprint_ID)
    for value in Record:
        x = value[0]
        Record_Dictionary['Status_Channel_'+str(x)] = x
    b = set(Record_Dictionary.values())
    print(b)
    if len(a)!= len(b):
        for a in range (0,_Number_of_Channels[Footprint_ID+str('_Channels')]):
            conn001.modules.IBM_Edit_Server.Deallocate_Frequency(Allocated_Frequency[Footprint_ID+str('_Channel_')+str(a + 1)],Footprint_ID)
        Request_Allocation(Satellite_ID,Footprint_ID,_Threshold_Frequency[Satellite_ID+'_'+Footprint_ID+'_Threshold_Frequency'],_Number_of_Channels[Footprint_ID+str('_Channels')])

def Request_Rainrate(Footprint_ID):
    global _Rainrate
    _Rainrate = {}
    _Rainrate = conn001.modules.IBM_Edit_Server.Fetch_Rainrate(_Footprint_ID)
    print(_Rainrate)

def Set_Current_Rainrate(Footprint_ID):
    global _Current_Rainrate
    _Current_Rainrate = {}
    _Current_Rainrate[Footprint_ID+str('_Current_Rainrate')] = _Rainrate[Footprint_ID+str('_Rainrate')]

def Request_Specific_Attenuation(Footprint_ID,Footprint_Theta,Footprint_Tao):
    global _Specific_Attenuation
    global _Frequency
    _Specific_Attenuation = {}
    _Frequency = {}
    _Frequency = conn001.modules.IBM_Edit_Server.Frequency(Footprint_ID)
    _Specific_Attenuation = conn001.modules.IBM_Edit_Server.Calculate_Specific_Attenuation(Footprint_ID,Footprint_Theta,Footprint_Tao)
    print(_Specific_Attenuation)
    print(_Frequency)

def Calculate_Attenuation(Footprint_ID):
    global _Threshold_Frequency
    _Threshold_Frequency = {}
    _Attenuation = {}
    _Attenuation_001 = {}
    _Effective_Pathlength = {}
    _L = {}
    _X = {}
    V_Reduction_Factor = {}
    _H_Reduction_Factor = {}
    i = 1
    value = Footprint_ID
    if _Rainrate[value+'_Rainrate'] == 0:
        _Threshold_Frequency[Satellite_ID+'_'+value+'_Threshold_Frequency'] = 50
    else:
        Request_Specific_Attenuation(value,_Footprint_Theta[value+'_Theta'],_Footprint_Tao[value+'_Tao'])
        for key in _Specific_Attenuation.keys():
            Frequency = _Frequency[str('Frequency')+str(i)]
            _Attenuation[str('Attenuation_')+key] = _Specific_Attenuation[key]*_Slant_Path[value+str('_Ls')]*_Rainrate[value+str('_Rainrate')]
            _Attenuation_001[str('A_0.001_'+key)] = _Attenuation[str('Attenuation_')+key]*2.13885
            if _Attenuation_001[str('A_0.001_'+key)] <= _Threshold_Attenuation[value+str('_Th_Attenuation')]:
                _Threshold_Frequency[Satellite_ID+'_'+value+'_Threshold_Frequency'] = Frequency
            i += 1
        print(_Attenuation_001)
    print(_Threshold_Frequency)

def Request_Allocation(Satellite_ID,Footprint_ID,Threshold_Frequency,Number_of_Channels,Previously_Priviledged):
    global _Allocated_Frequency
    _Allocated_Frequency = {}
    _Allocated_Frequency = conn001.modules.IBM_Edit_Server.Allocate_Frequencies(Satellite_ID,Footprint_ID,Threshold_Frequency,Number_of_Channels,Previously_Priviledged)
    print(_Allocated_Frequency)

def main():
    global _Current_Rainrate
    global _Footprint_ID
    global Allocated_Frequency
    global Previously_Priviledged
    Previously_Priviledged = False
    Allocated_Frequency = {}
    Initialize_Satellite()
    Calculate_Rain_Height()
    print(_Rain_Height)
    Calculate_Slant_Path()
    print(_Slant_Path)
    Calculate_Horizontal_Projection()
    print(_Horizontal_Projection)
    Request_Rainrate(_Footprint_ID)
    for value in _Footprint_ID.values():
        x = str(value)
        Calculate_Attenuation (value)
        Request_Allocation(Satellite_ID,x,_Threshold_Frequency[Satellite_ID+'_'+value+'_Threshold_Frequency'],_Number_of_Channels[x+'_Channels'],Previously_Priviledged)
        for a in range (0,_Number_of_Channels[value+str('_Channels')]):
            Allocated_Frequency[value+str('_Channel_')+str(a + 1)] = _Allocated_Frequency[Satellite_ID+'_'+value+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)]
        print(Allocated_Frequency)
        Set_Current_Rainrate(value)
    sleep (30)
    while True:
        Request_Rainrate(_Footprint_ID)
        for value in _Footprint_ID.values():
            x = str(value)
            if (_Rainrate[value+str('_Rainrate')] - _Current_Rainrate[value+str('_Current_Rainrate')]) > 0.5:
                Calculate_Attenuation (value)
                for a in range (0,_Number_of_Channels[value+str('_Channels')]):
                    conn001.modules.IBM_Edit_Server.Deallocate_Frequency(Allocated_Frequency[value+str('_Channel_')+str(a + 1)],value)
                Request_Allocation(Satellite_ID,x,_Threshold_Frequency[str(Satellite_ID)+'_'+str(value)+str('_Threshold_Frequency')],_Number_of_Channels[value+str('_Channels')],Previously_Priviledged)
                for a in range (0,_Number_of_Channels[value+str('_Channels')]):
                    Allocated_Frequency[value+str('_Channel_')+str(a + 1)] = _Allocated_Frequency[Satellite_ID+'_'+value+str('_Allocated_Frequency_')+str('Channel_')+str(a + 1)]
                Set_Current_Rainrate(value)
                print(Allocated_Frequency)
                if Previously_Priviledged == True:
                    Previously_Priviledged = False
            else:
                Set_Current_Rainrate(value)
                Check_Allocation_Status(Satellite_ID,value)
            if (_Current_Rainrate[value+str('_Current_Rainrate')] - _Rainrate[value+'_Rainrate']) > 0.5:
                Previously_Priviledged = True
                for a in range (0,_Number_of_Channels[value+'_no. of Channels']):
                    conn001.modules.IBM_Edit_Server.Update_Privileged_Dictionary(Satellite_ID,value,Allocated_Frequency[value+str('_Channel_')+str(a + 1)],a)
        sleep (30)
main()
