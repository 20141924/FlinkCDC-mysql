import org.apache.flink.table.functions.ScalarFunction;

public class replace extends ScalarFunction {
    public String eval(String a,String b,String c){
        String result;
        if(a.equals(b)){
            result=c;

        }
        else {
            result=a;
        }
        return result;
    }


}
