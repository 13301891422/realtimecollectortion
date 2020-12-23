package util;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2020/12/23 17:46:54
 */
public class StrUtils {

    public static void main(String[] args) {
        System.out.println(clickhouseInsertValue(new String[]{"name", "age"},"test"));
    }

    public static String clickhouseInsertValue(String[] tableColums, String tablename) {
        StringBuilder tableCoStr = new StringBuilder();
        String insertStr = "";
        for (int i = 0; i < tableColums.length; i++) {
            if (i == 0) {
                tableCoStr.append(tableColums[i]);
            }
            tableCoStr.append(",").append(tableColums[i]);
        }

        insertStr = "insert into " + tablename + " values" + "("+tableCoStr+")";

        return insertStr;
    }


}
