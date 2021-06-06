~Version
 VERS              .         2.0                           : CWLS LOG ASCII STANDARD - VERSION 2.0
 WRAP              .         NO                            : ONE LINE PER DEPTH STEP
 DLM               .         SPACE                         : DELIMITING CHARACTER(SPACE TAB OR COMMA)
~Well Information
#_______________________________________________________________________________
#
#PARAMETER_NAME    .UNIT     VALUE                         : DESCRIPTION
#_______________________________________________________________________________
STRT               .s        16620                         : First reference value
STOP               .s        17100                         : Last reference value
STEP               .s        60                            : Step increment
NULL               .         -9999                         : Missing value
COMP               .                                       : Company
WELL               .         616                           : Well name
FLD                .                                       : Field
LOC                .                                       : Location
CNTY               .                                       : County
STAT               .                                       : State
CTRY               .                                       : Country
SRVC               .                                       : Service Company
DATE               .                                       : Date
API                .                                       : API number
PROJECT_UPGRADE_TIME .         2021-03-18T21_24_50           : 
group              .         Good                          : 
~Parameter Information Block
#_______________________________________________________________________________
#
#PARAMETER_NAME    .UNIT     VALUE                         : DESCRIPTION
#_______________________________________________________________________________
SET                .         D7025_T                       : 
OTHER              .         Kedr.exe версия 2.1.2.32      : 
OTHER_10           .         TM.GRD = 0.00404*TM$.UE + 0.07: 
OTHER_11           .         MN.ATM = (6.33279e-006*TM.GRD + 0.02401)*(MN$.UE + 2.72*TM.GRD + -2018): 
OTHER_12           .         GK.MKR/H = GK$.UE / 43.5489 + 0: 
OTHER_13           .         RES.SM = 0.00476*CRES.UE - 1.51709: 
OTHER_14           .         PL.GSM3 = 0 + 0 * (PL.UE / 1) +  0 * (PL.UE / 1)^2: 
OTHER_15           .         Постоянная времени ГГП = 6 сек: 
OTHER_16           .         Постоянная времени ГК = 6 сек : 
OTHER_2            .         KedrData.dll версия 3.2.2.14  : 
OTHER_3            .         LasInf.dll версия 3.2.0.13    : 
OTHER_4            .         Драйвер версия 1.2.0.5        : 
OTHER_5            .         Прибор_ КСА-M1                : 
OTHER_6            .         Номер прибора_ 155+р204       : 
OTHER_7            .         Режим_ КСА-М1 + ГГП + Резистивиметр: 
OTHER_8            .         Форма_ 223\2                  : 
OTHER_9            .         Тарировки_                    : 
PROJECT_UPGRADE_TIME .         2021-03-18T21_24_50           : 
TLFamily_CRES      .                                       : 
TLFamily_DEPT      .         Measured Depth                : 
TLFamily_GK        .         Gamma Ray                     : 
TLFamily_GK$       .                                       : 
TLFamily_LM        .         Casing Collar locator         : 
TLFamily_METKA     .                                       : 
TLFamily_MN        .                                       : 
TLFamily_MN$       .                                       : 
TLFamily_PL        .                                       : 
TLFamily_PL_2      .                                       : 
TLFamily_RCOR      .                                       : 
TLFamily_RES       .         Mud Resistivity               : 
TLFamily_SPID      .         Spontaneous Potential         : 
TLFamily_STD       .                                       : 
TLFamily_TIME      .         Time                          : 
TLFamily_TM        .         Temperature                   : 
TLFamily_TM$       .         Temperature                   : 
TLFamily_VGD       .         Gas Volume                    : 
group              .         prim                          : 
~Curve Information
#_______________________________________________________________________________
#
#LOGNAME           .UNIT     LOG_ID                        : DESCRIPTION
#_______________________________________________________________________________
TIME               .s                                      : 1 Время, сек -    0.0 см 
CRES               .UE                                     : 9 Проводимость, усл.ед. -  222.0 см 
DEPT               .M                                      : 4 Глубина, м -    0.0 см 
GK                 .MKR/H                                  : 16 Гамма-активность, мкр./ч -  106.0 см 
GK$                .UE                                     : 13 Гамма-активность, усл.ед. -  106.0 см 
LM                 .                                       : 11 Локатор муфт,  -   16.0 см 
METKA              .                                       : 3 Метки,  -    0.0 см 
MN                 .ATM                                    : 15 Давление, атм. -  160.0 см 
MN$                .UE                                     : 7 Давление, усл.ед. -  160.0 см 
PL                 .UE                                     : 10 Плотность, усл.ед. -  270.0 см 
PL_2               .GSM3                                   : 18 Плотность, г./см3 -  270.0 см 
RCOR               .CM/KM                                  : 5 Коррекция по ролику, см/км -    0.0 см 
RES                .SM                                     : 17 Проводимость,  -  222.0 см 
SPID               .M/H                                    : 2 Скорость, м/час -    0.0 см 
STD                .                                       : 12 Приток,  -  170.0 см 
TM                 .GRD                                    : 14 Температура, °C -  180.0 см 
TM$                .UE                                     : 6 Температура, усл.ед. -  180.0 см 
VGD                .                                       : 8 Влагомер,  -  170.0 см 
~Ascii
16620.0    -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999      -9999     
16680      1258       2645.66    9.16       395        103        0          372.127    17125      14         0          0          4.471      0          11388      58.149     14376      6698      
16740      1256       2645.66    9.56       413        148        0          373.735    17190      14         0          0          4.4622     0          11405      58.23      14396      6693      
16800      1255       2645.66    9.7        422        146        0          372.739    17149      14         0          0          4.4585     0          11416      58.29      14411      6698      
16860      1255       2645.66    9.47       407        147        0          373.893    17196      14         0          0          4.4567     0          11425      58.359     14428      6694      
16920      1257       2645.66    9.74       426        157        0          373.457    17177      14         0          0          4.4684     0          11437      58.42      14443      6706      
16980      1260       2645.66    9.61       417        160        0          374.192    17207      14         0          0          4.4794     0          11447      58.48      14458      6696      
17040      1266       2645.66    9.68       423        164        0          369.376    17009      14         0          0          4.5071     0          11459      58.529     14470      6691      
17100      1226       2645.66    10.36      449        350        0          243.355    11843      14         0          0          4.3186     0          11361      58.012     14342      6653      
