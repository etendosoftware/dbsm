import org.openbravo.ddlutils.task.ExportDataXMLMod;
import java.io.*;

public class TestExport{


  public static void main(String args[])
  {
  org.openbravo.ddlutils.task.ExportDataXMLMod exp=new org.openbravo.ddlutils.task.ExportDataXMLMod();
    System.out.println("hola");
    exp.setDriver("org.postgresql.Driver");
    exp.setUrl("jdbc:postgresql://localhost:5433/trunk");
exp.setUser("tad");
exp.setPassword("tad");
exp.setExcludeobjects("com.openbravo.db.OpenbravoExcludeFilter");
exp.setFilter("com.openbravo.db.OpenbravoMetadataFilter");
exp.setOutput(new File("sourcedata"));
exp.setCodeRevision("8027");
exp.execute();
  }

}
