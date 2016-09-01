using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using System.IO;

namespace Newegg.BigData.DataLoaderService
{
    public class Helper
    {
        #region Main Process
        public static void SyncData()
        {
            try
            {
                //1. Get StartDate and EndDate from Utilities.Con_DateRangeFilePath
                SetDateRange(true);
                DateTime startDate, endDate;
                GetDateRange(out startDate, out endDate);

                //2. Separate StartDate&EndDate to List<(StartTime, EndTime)>, where StartTime is every single day's start time, and EndTime is the end time
                List<Tuple<DateTime, DateTime>> periodList;
                periodList = SeparateDateRange(startDate, endDate);
                if (periodList != null && periodList.Count > 0)
                {
                    //Threading task list
                    List<Task> workingTaskList = new List<Task>();

                    //3. For Each (StartTime, EndTime):
                    foreach (Tuple<DateTime, DateTime> period in periodList)
                    {
                        DateTime startTime = period.Item1;
                        DateTime endTime = period.Item2;
                        string startTimeStr = startTime.ToString("yyyy/MM/dd HH:mm:ss.fff");
                        string endTimeStr = endTime.ToString("yyyy/MM/dd HH:mm:ss.fff");
                        //3-1. Get DataTable from S7OVSDB04.Mis.dbo.IM_ItemPriceInfoChangeHistory as D1,
                        //     and get DataTable from ABS_SQL.ItemMaintain.dbo.PriceInfoHistory as D2 and D3
                        //     Use Utilities.Con_CountryCodeList as target country code
                        List<DataTable> combinationList;     // first DataTable is D1, second is D2 + D3, each DataTable contains only two columns:ItemNumber and CountryCode
                        combinationList = Logic.GetItemCountryCombination(startTime, endTime);

                        if (combinationList == null || combinationList.Count != 2) continue;

                        //3-2. Organize combinationList to Dictionary<CountryCode,List<ItemNumber>> for later to dispatch to threads to query sql server
                        Dictionary<string, List<string>> countryItemList = Logic.OrganizeItemCountryCombination(combinationList);
                        LogCountryItemCountInfo(startTimeStr, endTimeStr, countryItemList);
                        if (countryItemList == null || countryItemList.Count <= 0) continue;

                        foreach (KeyValuePair<string, List<string>> pair in countryItemList)
                        {
                            //USA or USB or CAN
                            string countryCode = pair.Key;
                            List<string> totalItemList = pair.Value;

                            //3-3. Organize totalItemList to Queue<List<string>>
                            //     Each List<string> in the Queue contains the items for a thread to process
                            Queue<List<string>> itemListQueue = Logic.SeparateItemNumberListForThreading(totalItemList);
                            LogThreadingItemCountInfo(startTimeStr, endTimeStr, countryCode, itemListQueue);
                            if (itemListQueue == null || itemListQueue.Count <= 0) continue;
                            //     ,and then dispatch to thread function

                            while (itemListQueue.Count > 0)
                            {
                                if (workingTaskList.Count < Utilities.CON_ThreadCount)
                                {
                                    List<string> threadItemList = itemListQueue.Dequeue();
                                    if (threadItemList == null || threadItemList.Count <= 0) continue;

                                    //Thread Process
                                    Task task = Task.Factory.StartNew(() =>
                                    {
                                        try
                                        {
                                            //3-4. For give time period, country code, and item number list, countrycode=> get PriceChangeHistory from IM_ItemPriceInfoChangeHistory as D1,
                                            //     from PriceInfoHistory as D2 and D3
                                            //     The key in dictionary represents itemnumber, and value is array of D1, D2, D3
                                            //     The column name in DataRow D1, D2, D3 need to be the with ItemPriceChangeCombineSolr's property names
                                            //     D1, D2, and D3 all have ItemNumber,CountryCode,CompanyCode columns
                                            List<DataTable> forItemPirceChangeSolr = new List<DataTable>();
                                            Dictionary<string, List<DataRow>[]> priceChangeHistory = Logic.GetPriceChangeHistory(startTime, endTime, countryCode, threadItemList, ref forItemPirceChangeSolr);

                                            bool isSuccess1 = true;
                                            if (Utilities.CON_ItemPriceChangeCombineSolrFlag)
                                            {
                                                //3-5. For given D1, D2, D3, query solr to get all item's Solr data from ItemPriceChangeCombineSolr as D4.
                                                //     Organize D1, D2, D3, D4 and then save to Solr
                                                isSuccess1 = Logic.SyncToItemPriceChangeCombineSolr(startTime, endTime, countryCode, priceChangeHistory);
                                            }
                                            bool isSuccess2 = true;
                                            if (Utilities.CON_ItemPriceChangeSolrFlag)
                                            {
                                                //3-6. For given D1, D2, D3, just save all item's data to ItemPriceChangeSolr
                                                isSuccess2 = Logic.SyncToItemPriceChangeSolr(startTime, endTime, countryCode, forItemPirceChangeSolr);
                                            }

                                            if (!isSuccess1 || !isSuccess2)
                                            {
                                                Logic.FailType failType;
                                                if (!isSuccess1 && !isSuccess1)
                                                {
                                                    failType = Logic.FailType.Both;
                                                }
                                                else if (!isSuccess1)
                                                {
                                                    failType = Logic.FailType.ItemPriceChangeCombine;
                                                }
                                                else
                                                {
                                                    failType = Logic.FailType.ItemPriceChange;
                                                }
                                                string failLog = string.Concat(failType, "|", startTimeStr, "|", endTimeStr, "|", countryCode, "|", string.Join(",", threadItemList));
                                            }
                                            else
                                            {
                                                 TraceProgramDebugLog(string.Format("PriceChangeHistoryService thread save success! Period:{0}~{1}, CountryCode:{2}, ItemNumbers:{3}", startTimeStr, endTimeStr, countryCode, string.Join(",", threadItemList)));
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            TraceLog(System.Reflection.MethodBase.GetCurrentMethod(), e);
                                            TraceProgramDebugLog(string.Format("PriceChangeHistoryService thread execution error! Period:{0}~{1}, CountryCode:{2}, ItemNumbers:{3}", startTimeStr, endTimeStr, countryCode, string.Join(",", threadItemList)));
                                        }
                                    });

                                    workingTaskList.Add(task);
                                }
                                else
                                {
                                    Task[] taskArray = workingTaskList.ToArray();
                                    int index = Task.WaitAny(taskArray);
                                    workingTaskList.Remove(taskArray[index]);
                                }
                            }
                        }
                    }

                    //wait all task to complete here
                    if (workingTaskList.Count > 0) Task.WaitAll(workingTaskList.ToArray());
                }

                SetDateRange(false);
            }
            catch (Exception ex)
            {
                TraceLog(System.Reflection.MethodBase.GetCurrentMethod(), ex);
            }
        }
        #endregion

        #region Common Function
        public static void GetDateRange(out DateTime startDate, out DateTime endDate)
        {
            //TODO1
            Tuple<string, string> dateStrs = GetDateRangeStrFromFile();
            startDate = DateTime.Parse(dateStrs.Item1);
            endDate = DateTime.Parse(dateStrs.Item2);
        }

        public static Tuple<string, string> GetDateRangeStrFromFile()
        {
            DateTime startDate = DateTime.MinValue;
            DateTime endDate = DateTime.MinValue;
            string line1 = "";
            string line2 = "";

            string inputPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Utilities.Con_DateRangeFilePath);
            if (string.IsNullOrEmpty(inputPath))
                return new Tuple<string, string>(line1, line2);

            try
            {
                using (FileStream fs = File.Open(inputPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BufferedStream bs = new BufferedStream(fs))
                using (StreamReader sr = new StreamReader(bs))
                {
                    line1 = sr.ReadLine() ?? "";
                    line2 = sr.ReadLine() ?? "";
                }
                return new Tuple<string, string>(line1, line2);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static void SetDateRange(bool isStart)
        {
            Tuple<string, string> datesStr = GetDateRangeStrFromFile();
            string line1 = datesStr.Item1;
            string line2 = datesStr.Item2;

            if (isStart)
            {
                if (!string.IsNullOrEmpty(line1) && string.IsNullOrEmpty(line2))//only has 1st line
                {
                    WriteToFile(line1, false);                     // 1st line is the origin start-time.
                    WriteToFile(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), true); // 2nd line is the current time.
                }
            }
            else
            {
                if (Utilities.Con_LoopFlag == true && !string.IsNullOrEmpty(line2))
                {
                    string newline1 = DateTime.Parse(line2).AddMilliseconds(1).ToString("yyyy-MM-dd HH:mm:ss.fff");
                    WriteToFile(newline1, false); // overwrite with the last end-time as next start-time, next end-time is null.
                }
            }
        }

        public static int WriteToFile(string content, bool append = true)
        {
            string filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Utilities.Con_DateRangeFilePath);

            if (string.IsNullOrEmpty(filePath)) return -1;

            try
            {
                using (StreamWriter writer = new StreamWriter(filePath, append)) // false for overwrite
                {
                    writer.WriteLine(content);
                }
                return 1;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static List<Tuple<DateTime, DateTime>> SeparateDateRange(DateTime startDate, DateTime endDate)
        {
            //TODO2
            List<Tuple<DateTime, DateTime>> result = new List<Tuple<DateTime, DateTime>>();
            int daysCnt = (int)endDate.Subtract(startDate).TotalDays;
            if (daysCnt <= 1)
            {
                result.AddRange(CheckLEOneDay(startDate, endDate));
            }
            else
            {
                while (daysCnt > 0)
                {
                    result.Add(new Tuple<DateTime, DateTime>(startDate, startDate.Date.AddDays(1).AddMilliseconds(-1)));
                    startDate = startDate.Date.AddDays(1);
                    if (daysCnt <= 1)
                    {
                        result.AddRange(CheckLEOneDay(startDate, endDate));
                    }
                    daysCnt -= 1;
                }
            }
            return result;
        }

        private static List<Tuple<DateTime, DateTime>> CheckLEOneDay(DateTime startDate, DateTime endDate)
        {
            List<Tuple<DateTime, DateTime>> result = new List<Tuple<DateTime, DateTime>>();
            if (startDate.Date == endDate.Date)
            {
                result.Add(new Tuple<DateTime, DateTime>(startDate, endDate));
            }
            else
            {
                result.Add(new Tuple<DateTime, DateTime>(startDate, startDate.Date.AddDays(1).AddMilliseconds(-1)));
                result.Add(new Tuple<DateTime, DateTime>(startDate.Date.AddDays(1), endDate));
            }
            return result;
        }
        #endregion
    }
}
