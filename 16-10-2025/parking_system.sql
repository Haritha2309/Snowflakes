INSERT INTO NY_PARKING_TABLE

(

Summons_Number, Plate_ID, Registration_State, Plate_Type, Issue_Date, Violation_Code, Vehicle_Body_Type, Vehicle_Make,

Issuing_Agency, Street_Code1, Street_Code2, Street_Code3, Vehicle_Expiration_Date, Violation_Location, Violation_Precinct,

Issuer_Precinct, Issuer_Code, Issuer_Command, Issuer_Squad, Violation_Time, Time_First_Observed, Violation_County,

Violation_In_Front_Of_Or_Opposite, House_Number, Street_Name, Intersecting_Street, Date_First_Observed, Law_Section,

Sub_Division, Violation_Legal_Code, Days_Parking_In_Effect, From_Hours_In_Effect, To_Hours_In_Effect, Vehicle_Color,

Unregistered_Vehicle, Vehicle_Year, Meter_Number, Feet_From_Curb, Violation_Post_Code, Violation_Description,

No_Standing_or_Stopping_Violation, Hydrant_Violation, Double_Parking_Violation, Latitude, Longitude, Community_Board,

Community_Council, Census_Tract, BIN, BBL, NTA

)

SELECT

Summons_Number, Plate_ID, Registration_State, Plate_Type, Issue_Date, Violation_Code, Vehicle_Body_Type, Vehicle_Make,

Issuing_Agency, Street_Code1, Street_Code2, Street_Code3, Vehicle_Expiration_Date, Violation_Location, Violation_Precinct,

Issuer_Precinct, Issuer_Code, Issuer_Command, Issuer_Squad, Violation_Time, Time_First_Observed, Violation_County,

Violation_In_Front_Of_Or_Opposite, House_Number, Street_Name, Intersecting_Street, Date_First_Observed, Law_Section,

Sub_Division, Violation_Legal_Code, Days_Parking_In_Effect, From_Hours_In_Effect, To_Hours_In_Effect, Vehicle_Color,

Unregistered_Vehicle, Vehicle_Year, Meter_Number, Feet_From_Curb, Violation_Post_Code, Violation_Description,

No_Standing_or_Stopping_Violation, Hydrant_Violation, Double_Parking_Violation, Latitude, Longitude, Community_Board,

Community_Council, Census_Tract, BIN, BBL, NTA

FROM NY_STREAM WHERE Registration_State='NY' AND metadata$action = 'INSERT';
 
INSERT INTO NJ_PARKING_TABLE

(

Summons_Number, Plate_ID, Registration_State, Plate_Type, Issue_Date, Violation_Code, Vehicle_Body_Type, Vehicle_Make,

Issuing_Agency, Street_Code1, Street_Code2, Street_Code3, Vehicle_Expiration_Date, Violation_Location, Violation_Precinct,

Issuer_Precinct, Issuer_Code, Issuer_Command, Issuer_Squad, Violation_Time, Time_First_Observed, Violation_County,

Violation_In_Front_Of_Or_Opposite, House_Number, Street_Name, Intersecting_Street, Date_First_Observed, Law_Section,

Sub_Division, Violation_Legal_Code, Days_Parking_In_Effect, From_Hours_In_Effect, To_Hours_In_Effect, Vehicle_Color,

Unregistered_Vehicle, Vehicle_Year, Meter_Number, Feet_From_Curb, Violation_Post_Code, Violation_Description,

No_Standing_or_Stopping_Violation, Hydrant_Violation, Double_Parking_Violation, Latitude, Longitude, Community_Board,

Community_Council, Census_Tract, BIN, BBL, NTA

)

SELECT

Summons_Number, Plate_ID, Registration_State, Plate_Type, Issue_Date, Violation_Code, Vehicle_Body_Type, Vehicle_Make,

Issuing_Agency, Street_Code1, Street_Code2, Street_Code3, Vehicle_Expiration_Date, Violation_Location, Violation_Precinct,

Issuer_Precinct, Issuer_Code, Issuer_Command, Issuer_Squad, Violation_Time, Time_First_Observed, Violation_County,

Violation_In_Front_Of_Or_Opposite, House_Number, Street_Name, Intersecting_Street, Date_First_Observed, Law_Section,

Sub_Division, Violation_Legal_Code, Days_Parking_In_Effect, From_Hours_In_Effect, To_Hours_In_Effect, Vehicle_Color,

Unregistered_Vehicle, Vehicle_Year, Meter_Number, Feet_From_Curb, Violation_Post_Code, Violation_Description,

No_Standing_or_Stopping_Violation, Hydrant_Violation, Double_Parking_Violation, Latitude, Longitude, Community_Board,

Community_Council, Census_Tract, BIN, BBL, NTA

FROM NJ_STREAM WHERE Registration_State='NJ' AND metadata$action = 'INSERT';
 