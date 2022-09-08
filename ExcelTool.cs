//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Reflection;

//using NPOI.HSSF.UserModel;
//using NPOI.SS.UserModel;

namespace WhizQ
{
    public class ExcelTool
    {
        public static string GetExcelColumnLetter(int index)
        {
            char[] chars = new char[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };
            index -= -1; //adjust so it matches 0-indexed array rather than 1-indexed column

            int quotient = index / 26;
            if (quotient > 0)
                return GetExcelColumnLetter(quotient) + chars[index % 26].ToString();
            else
                return chars[index % 26].ToString();
        }

        //public static HSSFWorkbook GetExcelWorkbook(dynamic lst, dynamic excelFormatList = null)
        //{
        //    var plst = new List<PropertyInfo>(lst[0].GetType().GetProperties());

        //    HSSFWorkbook hssfworkbook = new HSSFWorkbook();
        //    ISheet sheet1 = hssfworkbook.CreateSheet("Sheet1");

        //    IFont boldFont = hssfworkbook.CreateFont();
        //    boldFont.Boldweight = (short)FontBoldWeight.Bold;
        //    ICellStyle headerStyle = hssfworkbook.CreateCellStyle();
        //    headerStyle.SetFont(boldFont);
        //    //headerStyle.WrapText = true;
        //    //headerStyle.VerticalAlignment = VerticalAlignment.TOP;

        //    //ICellStyle centStyle = hssfworkbook.CreateCellStyle();
        //    //centStyle.DataFormat = hssfworkbook.CreateDataFormat().GetFormat("0.00");

        //    ICellStyle dateStyle = hssfworkbook.CreateCellStyle();
        //    dateStyle.DataFormat = hssfworkbook.CreateDataFormat().GetFormat("yyyy-MM-dd hh:mm:ss");

        //    //ICellStyle percentStyle = hssfworkbook.CreateCellStyle();
        //    //percentStyle.DataFormat = hssfworkbook.CreateDataFormat().GetFormat("0.00%");

        //    List<Dictionary<string, ICellStyle>> stylelst = new List<Dictionary<string, ICellStyle>>
        //    {
        //        new Dictionary<string, ICellStyle>() { { "yyyy-MM-dd hh:mm:ss", dateStyle } }
        //    };

        //    IRow headerRow = sheet1.CreateRow(0);
        //    int col = 0;
        //    List<dynamic> elst = new List<dynamic>();
        //    if (excelFormatList != null)
        //    {
        //        elst = new List<dynamic>(excelFormatList);
        //    }
        //    for (int c = 0; c < plst.Count; c++)
        //    {
        //        var felst = elst.Where(x => ((x.Field != null && x.Field.ToString().ToLower() == plst[c].Name.ToLower()) || (x.Column != null && Convert.ToInt32(x.Column) == c))).ToList();
        //        string headerText = Api.ToSentenceCase(plst[c].Name);
        //        if (felst.Count > 0)
        //        {
        //            if (felst[0].HeaderText != null && felst[0].HeaderText.ToString().Trim() != "" && (felst[0].Skip == null || !Convert.ToBoolean(felst[0].Skip)))
        //            {
        //                headerText = felst[0].HeaderText.ToString();
        //                ICell cell = headerRow.CreateCell(col);
        //                cell.SetCellValue(headerText);
        //                col += 1;
        //            }
        //        }
        //        else
        //        {
        //            ICell cell = headerRow.CreateCell(col);
        //            cell.SetCellValue(headerText);
        //            col += 1;
        //        }
        //    }
        //    for (int r = 0; r < lst.Count; r++)
        //    {
        //        IRow row = sheet1.CreateRow(r + 1);

        //        col = 0;
        //        for (int c = 0; c < plst.Count; c++)
        //        {
        //            bool skip = false;
        //            string dataFormat = null;
        //            var felst = elst.Where(x => ((x.Field != null && x.Field.ToString().ToLower() == plst[c].Name.ToLower()) || (x.Column != null && Convert.ToInt32(x.Column) == c))).ToList();
        //            if (felst.Count > 0)
        //            {
        //                if (felst[0].Skip != null && Convert.ToBoolean(felst[0].Skip))
        //                {
        //                    skip = true;
        //                }
        //                if (!skip && felst[0].DataFormat != null && felst[0].DataFormat.ToString().Trim() != "")
        //                {
        //                    dataFormat = felst[0].DataFormat.ToString();
        //                }
        //            }

        //            dynamic val = plst[c].GetValue(lst[r]);
        //            if (val != null && !skip)
        //            {
        //                ICell cell = row.CreateCell(col);
        //                bool isCustomFormat = false;
        //                if (!string.IsNullOrEmpty(dataFormat))
        //                {
        //                    isCustomFormat = true;
        //                    var fstylelst = stylelst.Where(x => x.ContainsKey(dataFormat)).ToList();
        //                    if (fstylelst.Count == 0)
        //                    {
        //                        ICellStyle customStyle = hssfworkbook.CreateCellStyle();
        //                        customStyle.DataFormat = hssfworkbook.CreateDataFormat().GetFormat(dataFormat);
        //                        stylelst.Add(new Dictionary<string, ICellStyle>() { { dataFormat, customStyle } });
        //                        cell.CellStyle = customStyle;
        //                    }
        //                    else
        //                    {
        //                        cell.CellStyle = fstylelst[0][dataFormat];
        //                    }
        //                }

        //                if (plst[c].PropertyType.FullName.Contains("Int") || plst[c].PropertyType.FullName.Contains("Float") ||
        //                    plst[c].PropertyType.FullName.Contains("Double") || plst[c].PropertyType.FullName.Contains("Decimal"))
        //                {
        //                    //if (plst[c].PropertyType.FullName.Contains("Float") || plst[c].PropertyType.FullName.Contains("Double") ||
        //                    //    plst[c].PropertyType.FullName.Contains("Decimal"))
        //                    //{
        //                    //    cell.CellStyle.DataFormat = centStyle.DataFormat;
        //                    //}
        //                    double dbl = Convert.ToDouble(val);
        //                    cell.SetCellValue(dbl);
        //                }
        //                else if (plst[c].PropertyType.FullName.Contains("DateTime"))
        //                {
        //                    if (!isCustomFormat)
        //                    {
        //                        cell.CellStyle = stylelst[0]["yyyy-MM-dd hh:mm:ss"];
        //                    }
        //                    cell.SetCellValue(val);
        //                }
        //                else
        //                {
        //                    cell.SetCellValue(val.ToString());
        //                }
        //            }

        //            if (!skip)
        //            {
        //                col += 1;
        //            }
        //        }
        //    }

        //    for (int c = 0; c < headerRow.Cells.Count; c++)
        //    {
        //        headerRow.Cells[c].CellStyle = headerStyle;
        //        sheet1.AutoSizeColumn(c);
        //    }

        //    return hssfworkbook;
        //}

        //public static byte[] GetExcelReportBytes(dynamic lst)
        //{
        //    HSSFWorkbook hssfworkbook = GetExcelWorkbook(lst);
        //    MemoryStream ms = new MemoryStream();
        //    hssfworkbook.Write(ms);
        //    return ms.ToArray();
        //}



        //public static void ExportExcel(dynamic lst, string filename)
        //{
        //    HttpContext.Current.Response.Clear();
        //    HttpContext.Current.Response.ContentType = "application/vnd.ms-excel";
        //    HttpContext.Current.Response.AddHeader("Content-Disposition", string.Format("attachment;filename={0}", filename));
        //    HttpContext.Current.Response.Clear();

        //    HSSFWorkbook hssfworkbook = GetExcelWorkbook(lst);

        //    MemoryStream ms = new MemoryStream();
        //    hssfworkbook.Write(ms);
        //    ms.WriteTo(HttpContext.Current.Response.OutputStream);
        //    HttpContext.Current.Response.End();
        //}

        //public static void ExportExcel(dynamic lst, dynamic excelFormatList, string filename)
        //{
        //    HttpContext.Current.Response.Clear();
        //    HttpContext.Current.Response.ContentType = "application/vnd.ms-excel";
        //    HttpContext.Current.Response.AddHeader("Content-Disposition", string.Format("attachment;filename={0}", filename));
        //    HttpContext.Current.Response.Clear();

        //    HSSFWorkbook hssfworkbook = GetExcelWorkbook(lst, excelFormatList);

        //    MemoryStream ms = new MemoryStream();
        //    hssfworkbook.Write(ms);
        //    ms.WriteTo(HttpContext.Current.Response.OutputStream);
        //    HttpContext.Current.Response.End();
        //}

        //public static void ExportExcel(HSSFWorkbook workbook, string filename)
        //{
        //    HttpContext.Current.Response.Clear();
        //    HttpContext.Current.Response.ContentType = "application/vnd.ms-excel";
        //    HttpContext.Current.Response.AddHeader("Content-Disposition", string.Format("attachment;filename={0}", filename));
        //    HttpContext.Current.Response.Clear();

        //    HSSFWorkbook hssfworkbook = workbook;

        //    MemoryStream ms = new MemoryStream();
        //    hssfworkbook.Write(ms);
        //    ms.WriteTo(HttpContext.Current.Response.OutputStream);
        //    HttpContext.Current.Response.End();
        //}
    }
}
