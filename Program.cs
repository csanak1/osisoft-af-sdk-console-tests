using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Time;
using OSIsoft.AF.UnitsOfMeasure;

namespace PIDataQueryUsingAFSDK
{
    class Program : IDisposable
    {
        #region Statics
        private const string _piServerName = "xxxxxx";
        //public const string _piServerIP = "xxx.xxx.xxx.xxx";
        private const string _piDatabaseString = "xxxxx";

        //private static readonly NetworkCredential piCred = new NetworkCredential(@"piadmin", "xyz", "emea");
        //private static readonly NetworkCredential piCred = new NetworkCredential(@"piinterface", "xyz", "emea");
        private static readonly NetworkCredential piCred = new NetworkCredential(@"pivisionservice", "xyz", "emea");

        private const string _dateFormat = "yyyy.MM.dd. HH:mm:ss";
        private const int _pointPageSize = 1000;

        private static PIServer piServer;
        private static PISystem piSystem;
        private static AFDatabase piDatabase;
        #endregion

        static void Main(string[] args)
        {
            DateTime start = DateTime.Now.AddDays(-1); //start date
            DateTime end = DateTime.Now; //end date

            //GUID string format: HHHHHHHH-HHHH-HHHH-HHHHHHHHHHHH
            string tagName = "BC.HCLCONV.FIC1420.MODE";
            int timeSpanSeconds = 600; //number of smaples for interpolated query every x seconds

            var specifiedTimes = new List<DateTime>
            {
                DateTime.Now,
                DateTime.Now.AddHours(-1),
                DateTime.Now.AddHours(-12),
                DateTime.Now.AddDays(-1)
            };

            ////////////////////////////INTERPOLATED QUERY FOR ONE TAG/////////////////////////////
            ///Simple interpolated query for one tag with a specified interval and seconds between two samples
            var afValues = QueryTagInterpolatedData(tagName, timeSpanSeconds, start, end);

            /////print
            PrintAFValues(afValues);

            ///Interpolated query for one tag with a specified sample dates. Returns as many results as the count of the input dates
            afValues = QueryTagInterpolatedData(tagName, specifiedTimes.ToArray());

            /////print
            PrintAFValues(afValues);
            ////////////////////////////END/////////////////////////////

            ////////////////////////////RAW VALUES QUERY/////////////////////////////
            ///Simple raw value query for one tag with a specified interval (start and end dates)
            afValues = QueryTagRawValues(tagName, start, end);

            /////print
            PrintAFValues(afValues);

            ///Query specified amount of raw values before or after the specified starttime
            afValues = QueryTagRawValues(tagName, start, 30, true);

            /////print
            PrintAFValues(afValues);
            ////////////////////////////END/////////////////////////////

            ////////////////////////////QUERY TAGS/////////////////////////////
            var tagNames = new HashSet<string>();

            string piPointFilter = "*.MODE"; //get pipoints (tags) ending with .MODE
            //string piPointFilter = "BC.C.DISP.*"; //get pipoints (tags) starting with BC.C.DISP.*
            //string piPointFilter = "*CHLOR*"; //get pipoints (tags) containing CHLOR
            //string piPointFilter = "*"; //get all pipoints (tags)

            ///For filtering we can use masks for example
            var piPoints = GetPIPoints(piPointFilter);

            foreach (var piPoint in piPoints)
            {
                tagNames.Add(piPoint.Name);
            }

            /////add random tags for test
            //tagNames.Add("BC.HCLCONV.FIC1420.MODE_TEST");
            //tagNames.Add("BC.CHLOR.MC2_LOAD.PV");
            //tagNames.Add("BC.VCM.CAL24DIFF2CPV3.PV");
            //tagNames.Add("BC.C.MDI.LOAD.PV");
            //tagNames.Add("");
            //tagNames.Add("");

            ////////////////////////////INTERPOLATED QUERY FOR MULTIPLE TAGS/////////////////////////////
            ///Simple interpolated query for multiple tags. There are other different approaches, maybe they would be faster too
            afValues = QueryTagsInterpolatedData(tagNames.ToArray(), timeSpanSeconds, start, end);

            /////print
            PrintAFValues(afValues);
            ///////////////////////////END///////////////////////////////

            ////////////////////////////QUERY HOURLY AVG FOR A TAG/////////////////////////////
            afValues = QueryHourlyAverage(tagName, start, end);

            /////print
            PrintAFValues(afValues);
            ///////////////////////////END///////////////////////////////

            ////////////////////////////GET UNITS OF MEASUREMENTS/////////////////////////////
            ///UOMs stored in a separate databse, not like in GE Proficy
            var uoms = GetUOMs();

            /////print: 
            PrintUOMs(uoms);
            ///////////////////////////END///////////////////////////////

            Console.ReadKey();
        }

        #region Query Tags
        public static PIPointList GetPIPoints(string piPointFilter, string sourceFilter = "")
        {
            Connect();

            //PIPoint.FindPIPoints method (with source filter too) doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPoint_FindPIPoints_5.htm
            var piPoints = new PIPointList(
                PIPoint.FindPIPoints(
                    piServer: piServer,
                    nameFilter: piPointFilter, //searching for tagname
                    sourceFilter: sourceFilter //searching for the tag source, it is optional in this case
                    ));

            Disconnect();

            return piPoints;
        }
        #endregion

        #region Get UOMs
        ///UOMs stored in a separate databse, not like in GE Proficy
        public static UOMs GetUOMs() //UOM is Unit of Measurement, from GE it is known as EGU (Engineering Unit)
        {
            Connect();

            //What is UOMDatabase class: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_UnitsOfMeasure_UOMDatabase.htm
            //UOMDatabase.UOMs property: https://docs.osisoft.com/bundle/af-sdk/page/html/P_OSIsoft_AF_UnitsOfMeasure_UOMDatabase_UOMs.htm
            //UOMs Class: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_UnitsOfMeasure_UOMs.htm
            var uoms = piSystem.UOMDatabase.UOMs;

            Disconnect();

            return uoms;
        }
        #endregion

        #region Query Raw Tag Values
        /// <summary>
        /// Simple raw value query for one tag with a specified interval (start and end dates)
        /// </summary>
        /// <returns>AFValues</returns>
        public static AFValues QueryTagRawValues(string tagName, DateTime start, DateTime end)
        {
            var afValues = new AFValues();

            Connect();

            var afStart = new AFTime(start);
            var afEnd = new AFTime(end);
            var afTimeRange = new AFTimeRange(afStart, afEnd);

            //AFBoundaryType enumeration doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFBoundaryType.htm
            var afBoundatyType = AFBoundaryType.Inside; //query points inside start and endtime

            var piTag = PIPoint.FindPIPoint(piServer, tagName);

            string piPointPath = $@"\\PIServer[{_piServerName}]\PIPoint[{piTag.Name}]";

            var afObject = AFObject.FindObject(piPointPath, piDatabase); 

            if (afObject is AFAttribute afAttribute)
            {
                //AFBoundaryType enumeration doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFBoundaryType.htm
                //AFData.RecordedValues method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Data_AFData_RecordedValue.htm
                afValues = afAttribute.Data.RecordedValues(
                    timeRange: afTimeRange,
                    boundaryType: afBoundatyType,
                    desiredUOM: null,
                    filterExpression: null,
                    includeFilteredValues: false);
            }

            Disconnect();

            return afValues;
        }

        /// <summary>
        /// Query specified amount of raw values before or after the specified starttime
        /// </summary>
        /// <returns>AFValues</returns>
        public static AFValues QueryTagRawValues(string tagName, DateTime start, int valueCount, bool goForward)
        {
            var afValues = new AFValues();

            Connect();

            var afStart = new AFTime(start);

            //AFBoundaryType enumeration doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFBoundaryType.htm
            var afBoundatyType = AFBoundaryType.Inside; //query points inside start and endtime

            var piTag = PIPoint.FindPIPoint(piServer, tagName);

            string piPointPath = $@"\\PIServer[{_piServerName}]\PIPoint[{piTag.Name}]";

            var afObject = AFObject.FindObject(piPointPath, piDatabase);

            if (afObject is AFAttribute afAttribute)
            {
                //AFData.RecordedValuesByCount method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Data_AFData_RecordedValuesByCount.htm
                afValues = afAttribute.Data.RecordedValuesByCount(
                    startTime: afStart,
                    count: valueCount,
                    boundaryType: afBoundatyType,
                    forward: goForward,
                    desiredUOM: null,
                    filterExpression: null,
                    includeFilteredValues: false);
            }

            Disconnect();

            return afValues;
        }
        #endregion

        #region Query Interpolated Tag Values
        /// <summary>
        /// Simple interpolated query for one tag with a specified interval and seconds between two samples
        /// </summary>
        /// <returns>AFValues</returns>
        public static AFValues QueryTagInterpolatedData(string tagName, int timeSpanSeconds, DateTime start, DateTime end)
        {
            var afValues = new AFValues();

            Connect();

            //What is AFTime (if input is DateTime): https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Time_AFTime__ctor.htm
            //Usage of AFTimeSpan: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Time_AFTimeSpan__ctor_2.htm
            //Usage of AFTimeRange (in this context): https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Time_AFTimeRange__ctor.htm
            var afStart = new AFTime(start);
            var afEnd = new AFTime(end);
            var afTimeRange = new AFTimeRange(afStart, afEnd);
            var afTimeSpan = new AFTimeSpan(TimeSpan.FromSeconds(timeSpanSeconds));

            //PIPoint.FindPIPoint Method (serching for name) doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPoint_FindPIPoint_1.htm
            //PIPoint.FindPIPoint Method (serching for pointID) doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPoint_FindPIPoint.htm
            var piTag = PIPoint.FindPIPoint(piServer, tagName); //obviusly searching for tagname here

            //Path syntax help: https://docs.osisoft.com/bundle/af-sdk/page/html/path-syntax-overview.htm
            string piPointPath = $@"\\PIServer[{_piServerName}]\PIPoint[{piTag.Name}]";

            //AFObject.FindObject method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_AFObject_FindObject_2.htm
            var afObject = AFObject.FindObject(piPointPath, piDatabase); //works, yay!
            //AFObject.FindObjects method doc (returns IList<AFObject>): https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_AFObject_FindObject_2.htm

            if (afObject is AFAttribute afAttribute)
            {
                //AFDataMethods enumeration doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFDataMethods.htm
                //AFData.InterpolatedValues method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Data_AFData_InterpolatedValues.htm
                afValues = afAttribute.Data.InterpolatedValues(
                    timeRange: afTimeRange,
                    interval: afTimeSpan,
                    desiredUOM: null,
                    filterExpression: null,
                    includeFilteredValues: false);
            }

            Disconnect();

            return afValues;
        }

        /// <summary>
        /// Interpolated query for one tag with a specified sample dates. Returns as many results as the count of the input dates
        /// </summary>
        /// <returns>AFValues</returns>
        public static AFValues QueryTagInterpolatedData(string tagName, DateTime[] specifiedTimes)
        {
            //It is possible to get interpolated values on specified datetimes using AFData.InterpolatedValuesAtTimes method: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Data_AFData_InterpolatedValuesAtTimes.htm

            var afTimes = new List<AFTime>();

            afTimes = specifiedTimes.Select(x => new AFTime() { }).ToList();

            var afValues = new AFValues();

            Connect();
            var piTag = PIPoint.FindPIPoint(piServer, tagName); //obviusly searching for tagname here

            //Path syntax help: https://docs.osisoft.com/bundle/af-sdk/page/html/path-syntax-overview.htm
            string piPointPath = $@"\\PIServer[{_piServerName}]\PIPoint[{piTag.Name}]";

            //AFObject.FindObject method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_AFObject_FindObject_2.htm
            var afObject = AFObject.FindObject(piPointPath, piDatabase); //works, yay!
            //AFObject.FindObjects method doc (returns IList<AFObject>): https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_AFObject_FindObject_2.htm

            if (afObject is AFAttribute afAttribute)
            {
                //AFData.InterpolatedValuesAtTimes method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Data_AFData_InterpolatedValuesAtTimes.htm
                afValues = afAttribute.Data.InterpolatedValuesAtTimes(
                    times: afTimes,
                    desiredUOM: null,
                    filterExpression: null,
                    includeFilteredValues: false);
            }

            Disconnect();

            return afValues;
        }

        /// <summary>
        /// Simple interpolated query for multiple tags. There are other different approaches, maybe they would be faster too
        /// </summary>
        /// <returns>AFValues</returns>
        public static AFValues QueryTagsInterpolatedData(string[] tagNames, int timeSpanSeconds, DateTime start, DateTime end)
        {
            var afValues = new AFValues();

            Connect();

            var afStart = new AFTime(start);
            var afEnd = new AFTime(end);
            var afTimeRange = new AFTimeRange(afStart, afEnd);
            var afTimeSpan = new AFTimeSpan(TimeSpan.FromSeconds(timeSpanSeconds));

            foreach (string tagName in tagNames)
            {
                var piTag = PIPoint.FindPIPoint(piServer, tagName);

                //Path syntax help: https://docs.osisoft.com/bundle/af-sdk/page/html/path-syntax-overview.htm
                string piPointPath = $@"\\PIServer[{_piServerName}]\PIPoint[{piTag.Name}]";
                string filterExpression = $@"'\\PIServer[{_piServerName}]\PIPoint[{piTag.Name}]'<0";

                var afObject = AFObject.FindObject(piPointPath, piDatabase); //works, yay!

                if (afObject is AFAttribute afAttribute)
                {
                    afValues = afAttribute.Data.InterpolatedValues(
                        timeRange: afTimeRange,
                        interval: afTimeSpan,
                        desiredUOM: null,
                        filterExpression: filterExpression,
                        includeFilteredValues: false);
                }
            }

            Disconnect();

            return afValues;
        }

        //TODO: QueryTagInterpolatedData using AFObject.FindObjects(IList<AFPathToken>)
        //public static AFValues QueryTagInterpolatedData(string[] tagNames, int timeSpanSeconds, DateTime start, DateTime end)
        //{
        //    var afValues = new AFValues();

        //    Connect();

        //    //TODO: here create afTokens as PathTokens
        //    //At the moment I don't understand the logic behind AFPathTokens. 

        //    //FindObjects documentation: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_AFObject_FindObjects.htm
        //    var afObjects = AFObject.FindObjects(afTokens, piDatabase); //returns null always :(
        //    var afStart = new AFTime(start);
        //    var afEnd = new AFTime(end);
        //    var afTimeRange = new AFTimeRange(afStart, afEnd);
        //    var afTimeSpan = new AFTimeSpan(TimeSpan.FromSeconds(timeSpanSeconds));

        //    if (afObjects is List<AFAttribute> afAttributes)
        //    {
        //        foreach (var afAttribute in afAttributes)
        //        {
        //            afValues = afAttribute.Data.InterpolatedValues(
        //                timeRange: afTimeRange,
        //                interval: afTimeSpan,
        //                desiredUOM: null,
        //                filterExpression: null,
        //                includeFilteredValues: false);
        //        }
        //    }

        //    Disconnect();

        //    return afValues;
        //}
        #endregion

        #region Query Summaries (Avg, Sum, etc.)
        /// <summary>
        /// Returns the hourly average of a tag between the specified interval
        /// </summary>
        /// <returns>AFValues</returns>
        /// This is concrete example for AFData.Summaries method
        public static AFValues QueryHourlyAverage(string tagName, DateTime start, DateTime end)
        {
            var afValues = new AFValues();
            var afSummaryValues = new Dictionary<AFSummaryTypes, AFValues>();

            Connect();

            var afStart = new AFTime(start);
            var afEnd = new AFTime(end);
            var afTimeRange = new AFTimeRange(afStart, afEnd);
            var afSummaryDuration = new AFTimeSpan(TimeSpan.FromHours(1)); //fix one hour, because we talk about hourly average

            //AFSummaryTypes enum doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFSummaryTypes.htm
            var afSummaryType = AFSummaryTypes.Average;

            //AFCalculationBasis enum doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFCalculationBasis.htm
            var afCalculationBasis = AFCalculationBasis.TimeWeighted;

            //AFTimestampCalculation enum doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFTimestampCalculation.htm
            var afTimeType = AFTimestampCalculation.EarliestTime;

            var piTag = PIPoint.FindPIPoint(piServer, tagName);

            string piPointPath = $@"\\PIServer[{_piServerName}]\PIPoint[{piTag.Name}]";

            var afObject = AFObject.FindObject(piPointPath, piDatabase);

            if (afObject is AFAttribute afAttribute)
            {
                //AFData.Summaries method doc (contains everything what is needed): https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Data_AFData_Summaries.htm
                afSummaryValues = afAttribute.Data.Summaries(
                    timeRange: afTimeRange,
                    summaryDuration: afSummaryDuration,
                    summaryType: afSummaryType,
                    calcBasis: afCalculationBasis,
                    timeType: afTimeType) as Dictionary<AFSummaryTypes, AFValues>;
            }

            foreach (var afSummaryValue in afSummaryValues[afSummaryType])
            {
                afValues.Add(afSummaryValue);
            }

            Disconnect();

            return afValues;
        }
        #endregion

        #region Server connection mngmnt
        private static bool IsConnected()
        {
            if (piSystem != null && piDatabase != null)
                return piSystem.ConnectionInfo.IsConnected && piServer.ConnectionInfo.IsConnected; //returns connection status, the piServer and piSystems should be checked separately
            else return false;
        }

        private static void Connect()
        {
            if (!IsConnected())
            {
                try
                {
                    //piServer = new PIServers().DefaultPIServer; //connect to the default PI server
                    piServer = new PIServers()[_piServerName]; //connect to the specified server

                    if (piServer == null)
                        throw new InvalidOperationException("PI Server was not found.");

                    //piServer.Connect(piCred);
                    piServer.Connect();
                    piSystem = piServer.PISystem; //trying to get the PI system

                    if (piSystem == null)
                        piSystem = new PISystems().DefaultPISystem; //connect to the default PI system

                    if (piSystem == null)
                        throw new InvalidOperationException("PI Systems was not found.");

                    piSystem.Connect(piCred);

                    piDatabase = piSystem.Databases[_piDatabaseString]; //specify PI system database

                    if (piDatabase == null)
                        throw new InvalidOperationException("PI Database was not found.");
                }
                catch (PIConnectionException ex)
                {
                    Console.WriteLine("PI connection Exception occured: " + ex.Message);
                    throw;
                }

                if (!IsConnected())
                {
                    throw new Exception("Unable connect to PI server.");
                }
            }
        }

        private static void Disconnect()
        {
            if (IsConnected())
            {
                piSystem.Disconnect();
                piServer.Disconnect();
            }
        }
        #endregion

        #region Others
        public override string ToString()
        {
            //TODO: Let's be nice and show some context regarding the data values that were retrieved
            


            return this.ToString();
        }

        private static string GetStringValue(AFValue afValue)
        {
            if (afValue.Value is AFEnumerationValue value)
            {
                return value.Name;
            }
            else
            {
                switch (Type.GetTypeCode(afValue.Value.GetType()))
                {
                    case TypeCode.Decimal:
                        return Convert.ToDecimal(afValue.Value).ToString("0.00");
                    case TypeCode.Double:
                        return Convert.ToDouble(afValue.Value).ToString("0.00");
                    case TypeCode.Single:
                        return Convert.ToSingle(afValue.Value).ToString("0.00");
                    case TypeCode.Boolean:
                        //return (bool)afValue.Value ? "GOOD" : "BAD";
                        return (bool)afValue.Value ? "TRUE" : "FALSE";
                    case TypeCode.Int32:
                        return afValue.Value.ToString();
                    case TypeCode.Int16:
                        return afValue.Value.ToString();
                    case TypeCode.String:
                        return afValue.Value.ToString();
                    case TypeCode.DateTime:
                        return Convert.ToDateTime(afValue.Value).ToString(_dateFormat);
                    default:
                        return string.Format("{0} - Unknown type", afValue.Value.GetType().ToString());
                        //throw new InvalidCastException("Type is not defined or unknown.");
                }
            }
        }

        public void Dispose() => piSystem.Disconnect(); //simply disconnecting

        public static float ConvertTimeStampToFloat(DateTime timeStamp) =>
            (float)timeStamp.ToOADate() * 1000000;

        private static void PrintAFValues(AFValues afValues)
        {
            if (afValues != null)
            {
                foreach (var afValue in afValues)
                {
                    Console.WriteLine("Tag: {0}, Timestamp (Local): {1}, Value: {2}, Quality: {3}", afValue.PIPoint, afValue.Timestamp.LocalTime, GetStringValue(afValue), afValue.IsGood.ToString());
                }
            }
            else
            {
                Console.WriteLine("Query returned result of NULL. Check Tag and query syntax!");
            }
        }

        private static void PrintUOMs(UOMs uoms)
        {
            if (uoms != null)
            {
                foreach (var uom in uoms)
                {
                    if(!uom.IsDeleted)
                        Console.WriteLine("Unit of measurement: {0}, Abbreviation: {1}, Class: {2}, Description: {3}", uom.Name, uom.Abbreviation, uom.Class, uom.Description);
                }
            }
            else
            {
                Console.WriteLine("Query returned result of NULL. Check UOMs.");
            }
        }
        #endregion
    }
}
