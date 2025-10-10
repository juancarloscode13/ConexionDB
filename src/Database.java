import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class Database {
    static String c_ruta_padre="";
    static String c_log_file="//log_data_base";
    static String c_tipo_info="I";
    static String c_tipo_error="E";
    static String c_tipo_aviso="W";

    private static void escribe_log(BufferedWriter v_log_buf, String v_tipo, String v_traza){
        DateFormat v_fecha_hora_actual=new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        Date v_fecha_actual=new Date();
        try{
            v_log_buf.write(v_fecha_hora_actual.format(v_fecha_actual)+" - "+v_tipo+" - "+v_traza+"\n");
            v_log_buf.flush();
        }catch (IOException e){
            System.out.println("Error IO en el fichero log: "+e.toString());
            System.exit(1);
        }
        catch (Exception e) {
            System.out.println("Error escribiendo en el fichero de log: "+e.toString());
            System.exit(1);
        }
    }

    private static Connection conecta_db(BufferedWriter v_log_buf){
        Connection v_conexion = null;
        escribe_log(v_log_buf, c_tipo_info, "Estableciendo conexión");

        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
        }catch (Exception e){
            System.out.println("SQLException: "+e.getMessage());
            escribe_log(v_log_buf, c_tipo_error, "Error estableciendo conexión.");
        }

        try{
            v_conexion = DriverManager.getConnection("jdbc:mysql:// 192.168.56.1:3306", "desarrollador", "contraseña");
            escribe_log(v_log_buf, c_tipo_info, "Conexión establecida.");
        }catch (SQLException e){
            System.out.println("SQLException: "+e.getMessage());
            System.out.println("SQLState: "+e.getSQLState());
            System.out.println("VendorError: "+e.getErrorCode());
            escribe_log(v_log_buf, c_tipo_error, "Error estableciendo conexión "+e.getMessage());
        }
        return v_conexion;
    }

    private static void imprimeInforme(ResultSet i_datos, BufferedWriter v_log_buf) throws SQLException{
        ResultSetMetaData v_rs_metadatos= i_datos.getMetaData();
        int v_num_cols=v_rs_metadatos.getColumnCount();
        escribe_log(v_log_buf, c_tipo_info, "Imprimiendo informe.");
        while(i_datos.next()){
            for (int i = 0; i <= v_num_cols ; i++) {
                if (i>1) System.out.println(" | ");
                System.out.println(i_datos.getString(i));
            }
            System.out.println("");
        }
        escribe_log(v_log_buf, c_tipo_info, "Informe impreso.");
    }

    private static void lanzaConsulta(Connection i_conexion, String i_consulta, BufferedWriter v_log_buf){
        Statement v_sentencia=null;
        ResultSet v_resultado=null;

        escribe_log(v_log_buf, c_tipo_info,"Lanzando consulta: "+i_consulta);

        try{
            v_sentencia=i_conexion.createStatement();
            v_resultado=v_sentencia.executeQuery(i_consulta);
            escribe_log(v_log_buf, c_tipo_info, "Consulta ejecutada");
            imprimeInforme(v_resultado, v_log_buf);
        }catch (SQLException e){
            System.out.println("SQLException: "+e.getMessage());
            System.out.println("SQLState: "+e.getSQLState());
            System.out.println("VendorError: "+e.getErrorCode());
        }
    }

    public static void main(String[] args) throws Exception{

    }
}
