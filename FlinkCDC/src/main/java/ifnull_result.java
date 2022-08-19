import org.apache.flink.table.functions.ScalarFunction;

public class ifnull_result extends ScalarFunction {
    public String eval(String a){
        String result;
        //判断是否为空
        if(a==null||a.length()==0){
            result="aa";
        }
        else {
            result=a;
        }
        return result;
    }

}
