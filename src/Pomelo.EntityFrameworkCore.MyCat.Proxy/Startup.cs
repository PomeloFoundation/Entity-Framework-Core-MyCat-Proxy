using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Diagnostics;
using System.Text;
using System.Xml;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Pomelo.EntityFrameworkCore.MyCat.Proxy
{
    public class Startup
    {
        public static string MyCatRoot = (JsonConvert.DeserializeObject<dynamic>(File.ReadAllText(Path.Combine(Directory.GetCurrentDirectory(), "config.json"))).MyCatRoot);
        public static bool IsUnder16 = File.ReadAllText(Path.Combine(MyCatRoot, "version.txt")).IndexOf("1.5") >= 0 || File.ReadAllText(Path.Combine(MyCatRoot, "version.txt")).IndexOf("1.4") >= 0 || File.ReadAllText(Path.Combine(MyCatRoot, "version.txt")).IndexOf("1.3") >= 0;

        public void ConfigureServices(IServiceCollection services)
        {
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.Run(async (context) =>
            {
                if (context.Request.Method == "POST")
                {
                    var username = context.Request.Form["Username"].ToString();
                    var password = context.Request.Form["Password"].ToString();
                    var schema = context.Request.Form["Schema"].ToString();
                    var datanodes = context.Request.Form["DataNodes"].ToString();
                    var database = context.Request.Form["Database"].ToString().Trim();

                    // Sign In
                    if (!SignIn(username, password, database))
                    {
                        // TODO: Limit the max trial times
                        context.Response.StatusCode = 401;
                        await context.Response.WriteAsync("Username or password is incorrect");
                        return;
                    }

                    // Patch the index_to_charset.properties file
                    string itc = File.ReadAllText(Path.Combine(MyCatRoot, "conf", "index_to_charset.properties"));
                    if (itc.IndexOf("192=") < 0)
                    {
                        itc += "\r\n192=utf8";
                        File.WriteAllText(Path.Combine(MyCatRoot, "conf", "index_to_charset.properties"), itc);
                    }

                    // Deserialize json
                    var Schema = JsonConvert.DeserializeObject<List<MyCatTable>>(schema);
                    var DataNodes = JsonConvert.DeserializeObject<List<MyCatDataNode>>(datanodes);

                    // Handle slice rules
                    GenerateRuleXml(Schema, DataNodes, database, IsUnder16);

                    // Handle schemas
                    foreach (var x in Schema)
                    {
                        // 1. Check the sequence
                        if (x.IsAutoIncrement)
                        {
                            // Read sequence_conf.properties
                            string sc = File.ReadAllText(Path.Combine(MyCatRoot, "conf", "sequence_conf.properties"));
                            if (sc.IndexOf($"{ x.TableName.ToUpper() }.HISIDS") < 0)
                                sc += $"\r\n{ x.TableName.ToUpper() }.HISIDS=";
                            if (sc.IndexOf($"{ x.TableName.ToUpper() }.MINID") < 0)
                                sc += $"\r\n{ x.TableName.ToUpper() }.MINID=1";
                            if (sc.IndexOf($"{ x.TableName.ToUpper() }.MAXID") < 0)
                                sc += $"\r\n{ x.TableName.ToUpper() }.MAXID=9223372036854775807";
                            if (sc.IndexOf($"{ x.TableName.ToUpper() }.CURID") < 0)
                                sc += $"\r\n{ x.TableName.ToUpper() }.CURID=0";
                            File.WriteAllText(Path.Combine(MyCatRoot, "conf", "sequence_conf.properties"), sc);
                        }
                    }

                    // 2. Generate schema.xml
                    GenerateSchemaXml(Schema, DataNodes, database);

                    // 3. Restart mycat
                    Process startProc;
                    if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
                        startProc = Process.Start(Path.Combine(MyCatRoot, "bin", "mycat.bat"), "start");
                    else
                        startProc = Process.Start(Path.Combine(MyCatRoot, "bin", "mycat"), "start");
                    startProc.WaitForExit();
                    await context.Response.WriteAsync("Ok");
                    return;
                }
                await context.Response.WriteAsync("Hello, this is mycat entity framework core proxy.");
            });
        }

        public string PatchXml(string XmlSchema, string Xml)
        {
            return Xml.Replace($"<mycat:{ XmlSchema }", $"<!DOCTYPE mycat:{ XmlSchema } SYSTEM \"{ XmlSchema }.dtd\"><mycat:{ XmlSchema }");
        }

        public void GenerateRuleXml(List<MyCatTable> Schema, List<MyCatDataNode> DataNodes, string database, bool IsUnder16)
        {
            foreach(var s in Schema)
            {
                if (s.Keys.Count() != 1)
                    continue;
                string ruleXml;
                string funcXml;
                if (IsUnder16)
                {
                    ruleXml = $@"
	<tableRule name=""{ database }_{ s.TableName }_rule"">
		<rule>
			<columns>{ string.Join(",", s.Keys.First()) }</columns>
			<algorithm>{ database }_{ s.TableName }_func</algorithm>
		</rule>
	</tableRule>
";
                    funcXml = $@"
	<function name=""{ database }_{ s.TableName }_func"" class=""org.opencloudb.route.function.PartitionByMod"">
		<property name=""count"">{ s.DataNodes.Count() }</property>
	</function>
";
                }
                else // 1.6+
                {
                    ruleXml = $@"
	<tableRule name=""{ database }_{ s.TableName }_rule"">
		<rule>
			<columns>{ string.Join(",", s.Keys.First()) }</columns>
			<algorithm>{ database }_{ s.TableName }_func</algorithm>
		</rule>
	</tableRule>
";
                    funcXml = $@"
	<function name=""{ database }_{ s.TableName }_func"" class=""io.mycat.route.function.PartitionByMod"">
		<property name=""count"">{ s.DataNodes.Count()}</property>
	</function>
";
                }

                using (var reader = XmlReader.Create(Path.Combine(MyCatRoot, "conf", "rule.xml"), new XmlReaderSettings { DtdProcessing = DtdProcessing.Ignore }))
                {
                    var xml = new XmlDocument();
                    xml.Load(reader);
                    var mycatRule = xml.ChildNodes.Cast<XmlNode>().Single(x => x.Name == "mycat:rule");
                    foreach (var x in mycatRule.ChildNodes.Cast<XmlNode>().Where(x => (x.Name == "tableRule" && x.Attributes["name"].Value == $"{ database }_{ s.TableName }_rule") || (x.Name == "function" && x.Attributes["name"].Value == $"{ database }_{ s.TableName }_func")).ToList())
                        mycatRule.RemoveChild(x);
                    mycatRule.InnerXml = ruleXml + mycatRule.InnerXml;
                    mycatRule.InnerXml += funcXml;
                    reader.Dispose();
                    File.WriteAllText(Path.Combine(MyCatRoot, "conf", "rule.xml"), PatchXml("rule", xml.OuterXml));
                }
            }
        }

        public void GenerateSchemaXml(List<MyCatTable>Schema, List<MyCatDataNode> DataNodes, string database)
        {
            var SchemaBuilder = new StringBuilder();
            SchemaBuilder.AppendLine($"\r\n    <schema name=\"{ database }\" checkSQLschema=\"false\">");
            foreach (var t in Schema)
            {
                if (t.Keys.Count() == 1)
                    SchemaBuilder.AppendLine($"        <table name=\"{ t.TableName }\" primaryKey=\"{ t.Keys.First() }\" dataNode=\"{ string.Join(",", t.DataNodes.Select(x => database + "_" + x)) }\" rule=\"{ database }_{ t.TableName }_rule\" { (t.IsAutoIncrement ? "autoIncrement=\"true\"" : "") } />");
                else
                    SchemaBuilder.AppendLine($"        <table name=\"{ t.TableName }\" primaryKey=\"{ string.Join(",", t.Keys) }\" dataNode=\"{ database }_dn0\" { (t.IsAutoIncrement ? "autoIncrement=\"true\"" : "") } />");
            }
            SchemaBuilder.AppendLine($"    </schema>");

            using (var reader = XmlReader.Create(Path.Combine(MyCatRoot, "conf", "schema.xml"), new XmlReaderSettings { DtdProcessing = DtdProcessing.Ignore }))
            {
                var xml = new XmlDocument();
                xml.Load(reader);
                // Appending schema tags
                var mycatSchema = xml.ChildNodes.Cast<XmlNode>().Single(x => x.Name == "mycat:schema");
                foreach (var x in mycatSchema.ChildNodes.Cast<XmlNode>()
                    .Where(x => x.Name == "schema" && x.Attributes["name"].Value == database
                    || x.Name == "schema" && x.Attributes["name"].Value == "TESTDB").ToList())
                    mycatSchema.RemoveChild(x);
                mycatSchema.InnerXml = SchemaBuilder.ToString() + mycatSchema.InnerXml;

                // Appending data node tags
                for (var i = 0; i < DataNodes.Count; i++)
                {
                    foreach (var x in mycatSchema.ChildNodes.Cast<XmlNode>()
                        .Where(x => x.Name == "dataNode" && x.Attributes["name"].Value == $"{ database }_dn{ i }"
                        || x.Name == "dataNode" && x.Attributes["name"].Value == $"dn1"
                        || x.Name == "dataNode" && x.Attributes["name"].Value == $"dn2"
                        || x.Name == "dataNode" && x.Attributes["name"].Value == $"dn3").ToList())
                        mycatSchema.RemoveChild(x);
                    var child = xml.CreateElement("dataNode");
                    child.SetAttribute("name", database + "_dn" + i);
                    child.SetAttribute("dataHost", database + "_host" + i);
                    child.SetAttribute("database", DataNodes[i].Master.Database);
                    var before = mycatSchema.ChildNodes.Cast<XmlNode>().Where(x => x.Name == "dataHost").FirstOrDefault();
                    if (before != null)
                        mycatSchema.InsertBefore(child, before);
                    else
                        mycatSchema.AppendChild(child);
                }

                // Appending data host tags
                string DataHostsXml;
                for (var i = 0; i < DataNodes.Count; i++)
                {
                    foreach(var x in mycatSchema.ChildNodes.Cast<XmlNode>()
                        .Where(x => x.Name == "dataHost" && x.Attributes["name"].Value == $"{ database }_host{ i }"
                        || x.Name == "dataHost" && x.Attributes["name"].Value == $"localhost1").ToList())
                        mycatSchema.RemoveChild(x);
                    if (DataNodes[i].Slave == null)
                        DataHostsXml = $@"<dataHost name=""{ database }_host{ i }"" maxCon=""1000"" minCon=""10"" balance=""0""
     writeType=""0"" dbType=""mysql"" dbDriver=""native"">
    <heartbeat>select user()</heartbeat>
    <writeHost host=""{ database }_hostM{ i }"" url=""{ DataNodes[i].Master.Host }:{ DataNodes[i].Master.Port }"" user=""{ DataNodes[i].Master.Username }"" password=""{ DataNodes[i].Master.Password }"" />
  </dataHost>";
                    else
                        DataHostsXml = $@"    <dataHost name=""{ database }_host{ i }"" maxCon=""1000"" minCon=""10"" balance=""1""
     writeType=""0"" dbType=""mysql"" dbDriver=""native"">
    <heartbeat>select user()</heartbeat>
    <writeHost host=""{ database }_hostM{ i }"" url=""{ DataNodes[i].Master.Host }:{ DataNodes[i].Master.Port }"" user=""{ DataNodes[i].Master.Username }"" password=""{ DataNodes[i].Master.Password }"">
        <readHost host=""{ database }_hostS{ i }"" url=""{ DataNodes[i].Slave.Host }:{ DataNodes[i].Slave.Port }"" user=""{ DataNodes[i].Slave.Username }"" password=""{ DataNodes[i].Slave.Password }"" />
    </writeHost>
  </dataHost>";
                    mycatSchema.InnerXml += DataHostsXml;
                }
                reader.Dispose();
                File.WriteAllText(Path.Combine(MyCatRoot, "conf", "schema.xml"), PatchXml("schema", xml.OuterXml));
            }
        }

        public bool SignIn(string Username, string Password, string Database)
        {
            lock(this)
            {
                using (var reader = XmlReader.Create(Path.Combine(MyCatRoot, "conf", "server.xml"), new XmlReaderSettings { DtdProcessing = DtdProcessing.Ignore }))
                {
                    var xml = new XmlDocument();
                    xml.Load(reader);
                    var users = xml.GetElementsByTagName("user").Cast<XmlNode>();
                    var user = users.SingleOrDefault(x => x.Attributes["name"].Value == Username);
                    if (user == null)
                        return false;
                    if (user.LastChild.InnerText.ToLower() == "true" && user.Attributes["name"].Value.ToLower() == "readonly")
                        return false;
                    if (user.FirstChild.InnerText == Password)
                    {
                        // Remove TESTDB from any users
                        foreach (var x in users)
                        {
                            var schemasOfX = x.ChildNodes.Cast<XmlNode>().Where(y => y.Name == "property" && y.Attributes["name"].Value == "schemas").Single();
                            if (schemasOfX.InnerText.IndexOf("TESTDB") >= 0)
                                schemasOfX.InnerText = schemasOfX.InnerText.Replace("TESTDB", "").Trim().Trim(',').Trim();
                        }

                        var flag = false;
                        var node = user.ChildNodes.Cast<XmlNode>().Single(x => x.Name == "property" && x.Attributes["name"].Value == "schemas");
                        var schemas = node.InnerText.Split(',').ToList();
                        if (!schemas.Any(x => x.Trim() == Database))
                            flag = true;

                        // Pause mycat service
                        Process stopProcess;
                        if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
                            stopProcess = Process.Start(Path.Combine(MyCatRoot, "bin", "mycat.bat"), "stop");
                        else
                            stopProcess = Process.Start(Path.Combine(MyCatRoot, "bin", "mycat"), "stop");
                        stopProcess.WaitForExit();

                        if (flag)
                        {
                            schemas.Add(Database);
                            schemas.Remove("TESTDB");
                            node.InnerText = string.Join(",", schemas);
                            var xmlContent = xml.OuterXml;
                           
                            reader.Dispose();
                            File.WriteAllText(Path.Combine(MyCatRoot, "conf", "server.xml"), PatchXml("server", xml.OuterXml));
                        }

                        return true;
                    }
                    return false;
                }
            }
        }
    }
}
