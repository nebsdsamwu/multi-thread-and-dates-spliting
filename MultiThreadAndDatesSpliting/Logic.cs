using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.IO;


namespace Business
{
    public class PriceChangeHistoryBL
    {
        //startTime, endTime is in the same day, included
        //List<DataTable> - first DataTable is D1, second is D2 + D3, each DataTable contains only two 

        #region getDatabyItemAndSynctoItemPriceChangeCombine
        #region Thread  Separate
        public static Queue<List<string>> SeparateItemNumberListForThreading(List<string> totalItemList)
        {
            Queue<List<string>> itemListQueue = new Queue<List<string>>();
            List<string> tempKeyList = new List<string>();
            int dividCount = totalItemList.Count / Utilities.CON_ItemNumberCountPerThread + 1;
            int totalCount = totalItemList.Count;
            for (int i = 0; i < dividCount; i++)
            {
                if (totalCount < Utilities.CON_ItemNumberCountPerThread)
                {
                    tempKeyList = totalItemList.GetRange(Utilities.CON_ItemNumberCountPerThread * i, totalCount);
                }
                else
                {
                    tempKeyList = totalItemList.GetRange(Utilities.CON_ItemNumberCountPerThread * i, Utilities.CON_ItemNumberCountPerThread);
                }
                totalCount = totalCount - Utilities.CON_ItemNumberCountPerThread;
                itemListQueue.Enqueue(tempKeyList);
            }
            return itemListQueue;
        }

        public static Queue<string> SeparateFailItemForThreading(List<string> itemsInfo)
        {
            //TODO3.3 Organize totalItemList to Queue<List<string>>
            //        Each List<string> in the Queue contains the items for a thread to process
            //        Use <add key="ItemNumberCountPerThread" value="100" /> (Utilities.CON_ItemNumberCountPerThread) to control how many ItemNumber's data will be processed by single thread
            Queue<string> failItemListQueue = new Queue<string>();
            foreach (string itemInfo in itemsInfo)
            {
                failItemListQueue.Enqueue(itemInfo);
            }
            return failItemListQueue;
        }
        #endregion
        #endregion
    }
}
