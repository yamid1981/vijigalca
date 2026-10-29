using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.ConstrainedExecution;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using Opc.Da;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;
using System.Runtime.InteropServices;

namespace WindowsFormsApp2
{

    public partial class Form1 : Form
    {

        // Один словарь для всех данных
        ConcurrentDictionary<string, TagData> tagsDataChas =  new ConcurrentDictionary<string, TagData>();
        ConcurrentDictionary<string, TagData> tagsDataDen = new ConcurrentDictionary<string, TagData>();
        Dictionary<string, dann_teg> теги = new Dictionary<string, dann_teg>();
        private const int SC_CLOSE = 0xF060;
        private const int MF_GRAYED = 0x1;
        [DllImport("user32.dll")]
        private static extern IntPtr GetSystemMenu(IntPtr hWnd, bool bRevert);
        [DllImport("user32.dll")]
        private static extern int EnableMenuItem(IntPtr hMenu, int wIDEnableItem, int wEnable);

        Stopwatch clock = new Stopwatch();
        private int t1_tik = 0;
        private int t15_sek = 0;
        private Server server1;
        private OpcCom.Factory fact1 = new OpcCom.Factory();
        private Subscription groupRead1;
        private SubscriptionState groupState1;
        private List<Item> itemsList1 = new List<Item>();

        private Server server2;
        private OpcCom.Factory fact2 = new OpcCom.Factory();
        private Subscription groupRead2;
        private SubscriptionState groupState2;
        private List<Item> itemsList2 = new List<Item>();

        private Server server3;
        private OpcCom.Factory fact3 = new OpcCom.Factory();
        private Subscription groupRead3;
        private SubscriptionState groupState3;
        private List<Item> itemsList3 = new List<Item>();

        ConcurrentDictionary<string, Single> tegi = new ConcurrentDictionary<string, Single>();
        ConcurrentDictionary<string, Single> tegi_chs    = new ConcurrentDictionary<string, Single>();
        ConcurrentDictionary<string, Single> tegi_den = new ConcurrentDictionary<string, Single>();
        ConcurrentDictionary<string, string> tegi_kach = new ConcurrentDictionary<string, string>();
        ConcurrentDictionary<string, DateTime> tegi_lastChange = new ConcurrentDictionary<string, DateTime>();
        ConcurrentDictionary<string, int> tegi_lastUnix = new ConcurrentDictionary<string, int>();

        int count_tegi_chs = 0;
        int count_tegi_den = 0;
        float p_250M;
        private string data_p_250m;
        //string server_name = "Data Source=SRV-ASKUT\\WINCC;Initial Catalog=yamid;User ID=klient;Password=1234567;workstation id=(local)WNetSDK;";
        // string server_name = "Data Source=192.168.5.136,1433;Initial Catalog=yamid;User ID=klient;Password=1234567;workstation id=(local)WNetSDK,encrypt=false;trustServerCertificate=false;";
        string server_name = "Data Source=SRV-ASCUT;Initial Catalog=yamid;User ID=klient;Password=1234567;TrustServerCertificate=True;";

        private int tik_p250m=0;
        private CancellationTokenSource cancelTokenSource;
        private int chas_tik=0;
        private int min_tik=0;
        private int rabota;

        
        public Form1()
        {

            InitializeComponent();
            Encoding win1251 = Encoding.GetEncoding("windows-1251");
            CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            try
            {
                //// Получаем все процессы с именем "node"
                var processes = Process.GetProcessesByName("node");
                if (processes.Length > 0) // Проверяем, существует ли процесс
                {
                    // Завершите процесс сервера
                    foreach (var process in processes)
                    {
                        process.Kill();
                        process.WaitForExit(); // Ждем завершения процесса
                    }
                }
                processes = Process.GetProcessesByName("DAS");
                if (processes.Length > 0) // Проверяем, существует ли процесс
                {
                    // Завершите процесс сервера
                    foreach (var process in processes)
                    {
                        process.Kill();
                        process.WaitForExit(); // Ждем завершения процесса
                    }
                }
                Thread.Sleep(1000);
/*                Process.Start("c:\\node_js\\запуск_сервера_80.bat");*/


                //сервер 1
                server1 = new Opc.Da.Server(fact1, null);
                server1.Url = new Opc.URL("opcda://localhost/InSAT.ModbusOPCServer.DA");
                server1.Connect();
                var con1 = server1.GetStatus();

                groupState1 = new Opc.Da.SubscriptionState();
                groupState1.Name = "myReadGroup";
                groupState1.UpdateRate = 1000;
                groupState1.Active = true;
                groupRead1 = (Opc.Da.Subscription)server1.CreateSubscription(groupState1);
                groupRead1.DataChanged += new DataChangedEventHandler(groupRead_DataChanged1);
                var list = File.ReadAllLines("c:\\teg1.txt", win1251).ToList();
                for (int i = 0; i < list.Count; i++)
                {
                    Item item = new Item();
                    item.ItemName = list[i];
                    tegi[list[i]] = 0;
                    теги[list[i]] = new dann_teg(list[i], 0f, 0);
                    tegi_den[list[i]] = 0;
                    tegi_chs[list[i]] = 0;
                    tegi_lastUnix[list[i]] = 0;
                    tegi_lastChange.TryAdd(list[i], DateTime.UtcNow);   

                    tegi_kach[list[i]] = "bad";
                    itemsList1.Add(item);
                }
                groupRead1.AddItems(itemsList1.ToArray());
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Ошибка  {ex.Message}");

            }

            try
            {
                //сервер 2
                server2 = new Opc.Da.Server(fact2, null);
                server2.Url = new Opc.URL("opcda://localhost/OPC.AlphaMera");
                server2.Connect();
                var con2 = server2.GetStatus();
                groupState2 = new Opc.Da.SubscriptionState();
                groupState2.Name = "myReadGroup";
                groupState2.UpdateRate = 5000;
                groupState2.Active = true;
                groupRead2 = (Opc.Da.Subscription)server2.CreateSubscription(groupState2);
                groupRead2.DataChanged += new DataChangedEventHandler(groupRead_DataChanged1);
                var list2 = File.ReadAllLines("c:\\teg2.txt", win1251).ToList();
                for (int i = 0; i < list2.Count; i++)
                {
                    Item item = new Item();
                    item.ItemName = list2[i];
                    tegi[list2[i]] = 0;
                    теги[list2[i]] = new dann_teg(list2[i], 0f, 0);
                    tegi_chs[list2[i]] = 0;
                    tegi_den[list2[i]] = 0;
                    tegi_lastUnix[list2[i]] = 0;
                    tegi_lastChange.TryAdd(list2[i], DateTime.UtcNow);
                    tegi_kach[list2[i]] = "bad";
                    itemsList2.Add(item);
                }
                groupRead2.AddItems(itemsList2.ToArray());
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Ошибка подключения к серверу 2 opcda://localhost/OPC.AlphaMera: {ex.Message}");

            }
            try
            {
                //сервер 3
                server3 = new Opc.Da.Server(fact3, null);
                server3.Url = new Opc.URL("opcda://localhost/Logika.DA.2");
                server3.Connect();
                var con3 = server3.GetStatus();
                groupState3 = new Opc.Da.SubscriptionState();
                groupState3.Name = "myReadGroup";
                groupState3.UpdateRate = 10000;
                groupState3.Active = true;
                groupRead3 = (Opc.Da.Subscription)server3.CreateSubscription(groupState3);
                groupRead3.DataChanged += new DataChangedEventHandler(groupRead_DataChanged1);
                var list3 = File.ReadAllLines("c:\\teg3.txt", win1251).ToList();
                for (int i = 0; i < list3.Count; i++)
                {
                    Item item = new Item();
                    item.ItemName = list3[i];
                    tegi[list3[i]] = 0;
                    теги[list3[i]] = new dann_teg(list3[i], 0f, 0);
                    tegi_chs[list3[i]] = 0;
                    tegi_den[list3[i]] = 0;
                    tegi_lastUnix[list3[i]] = 0;
                    tegi_lastChange.TryAdd(list3[i], DateTime.UtcNow);
                    tegi_kach[list3[i]] = "bad";
                    itemsList3.Add(item);
                }
                groupRead3.AddItems(itemsList3.ToArray());

                //инициализируем Арт Хим Пар

                теги["АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] = new dann_teg("АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1", 0f, 0);
                теги["АртВода.СКМ-2.Текущие параметры.Давление канал 1"] = new dann_teg("АртВода.СКМ-2.Текущие параметры.Давление канал 1", 0f, 0);

                теги["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] = new dann_teg("ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1", 0f, 0);
                теги["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"] = new dann_teg("ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1", 0f, 0);
                теги["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"] = new dann_teg("ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1", 0f, 0);

                теги["СПСеть.SPT961_1M.т1.154(P)"] = new dann_teg("СПСеть.SPT961_1M.т1.154(P)", 0f, 0);
                теги["СПСеть.SPT961_1M.т1.156(T)"] = new dann_teg("СПСеть.SPT961_1M.т1.156(T)", 0f, 0);
                теги["СПСеть.SPT961_1M.т1.157(G)"] = new dann_teg("СПСеть.SPT961_1M.т1.157(G)", 0f, 0);

            }
            catch (Exception ex)
            {
                MessageBox.Show($"Ошибка подключения к серверу 3 opcda://localhost/Logika.DA.2: {ex.Message}");

            }
            timer1.Start();

        }
        // Отключить кнопку "Закрыть"
        private void DisableCloseButton()
        {
            IntPtr hMenu = GetSystemMenu(this.Handle, false);
            EnableMenuItem(hMenu, SC_CLOSE, MF_GRAYED);
        }
        void UpdateKachFromUnix()
        {
            foreach (var tag in tegi.Keys)
            {
                if (tegi_lastUnix.ContainsKey(tag))
                {
                    DateTime lastUpdate = new DateTime(1970, 1, 1).AddSeconds(tegi_lastUnix[tag]);
                    bool isStale = (DateTime.UtcNow - lastUpdate).TotalHours >= 1;
                    tegi_kach[tag] = isStale ? "bad" : "good";
                }
                else
                {
                    tegi_kach[tag] = "bad";
                }
            }
        }

        void текущие_значения()
        {
            try
            {
                UpdateKachFromUnix();
                string t1 = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                string sql = "SELECT * FROM teg_online";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];

                    p_250M = (float)Math.Round(Convert.ToSingle(dt.Rows[498]["val"]), 1);
                    data_p_250m = dt.Rows[498]["teg"].ToString();
                    dt.Rows[499]["teg"] = t1;

                    for (int i = 0; i < tegi.Count; i++)
                    {
                        string _teg = tegi.Keys.ElementAt(i);
                        dt.Rows[i]["teg"] = _teg;
                        dt.Rows[i]["val"] = tegi[_teg];
                        dt.Rows[i]["kach"] = (tegi_kach[_teg] == "good");
                        dt.Rows[i]["error"] = tegi_lastUnix.ContainsKey(_teg) ? tegi_lastUnix[_teg] : 0;

                    }

                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    int updatedRows = adapter.Update(ds);

                    if (updatedRows == 0)
                    {
                        throw new Exception("No rows were updated.");
                    }

                    ds.Clear();
                    adapter.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }

        void groupRead_DataChanged1(object subscriptionHandle, object requestHandle, ItemValueResult[] values)
        {
            try
            {
                foreach (ItemValueResult itemValue in values)
                {
                    string tag = itemValue.ItemName;
                    float rawVal = Convert.ToSingle(itemValue.Value);
                    float newVal = rawVal;
                    int unixNow = (int)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;

                    bool glitch = false;

                    // выборочная фильтрация только для проблемного тега
                    if (tag == "ХимОчищВода.СКМ-2.Текущие параметры.Объемный расход канал 1")
                    {
                        glitch = IsGlitchZero(tag, rawVal);
                        if (glitch && tegi.ContainsKey(tag)) // Проверяем наличие ключа здесь
                        {
                            newVal = tegi[tag]; // игнорируем кратковременный ноль
                        }
                    }

                    if (!tegi.ContainsKey(tag))
                    {
                        tegi.TryAdd(tag, newVal); // Используем TryAdd для добавления нового элемента
                        tegi_lastChange.TryAdd(tag, DateTime.UtcNow); // Аналогично добавляем новое значение
                        tegi_lastUnix.TryAdd(tag, unixNow); // Добавляем Unix-время
                        tegi_kach.TryAdd(tag, glitch ? "suspect" : "good"); // Устанавливаем качество сигнала
                    }
                    else
                    {
                        float oldVal = tegi[tag];

                        if (oldVal != newVal)
                        {
                            tegi[tag] = newVal;
                            tegi_lastChange[tag] = DateTime.UtcNow;
                            tegi_lastUnix[tag] = unixNow;
                            tegi_kach[tag] = glitch ? "suspect" : "good";
                        }
                        else
                        {
                            TimeSpan delta = DateTime.UtcNow - tegi_lastChange[tag];//это строка 340
                            tegi_kach[tag] = delta.TotalHours >= 1 ? "bad" : "good";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }

        // вспомогательная функция
        bool IsGlitchZero(string tag, float newVal)
        {
            if (newVal != 0f || !tegi.ContainsKey(tag)) return false;

            float prevVal = tegi[tag];
            if (prevVal == 0f) return false;

            if (!tegi_lastChange.ContainsKey(tag)) return false;

            TimeSpan sinceLastChange = DateTime.UtcNow - tegi_lastChange[tag];
            return sinceLastChange.TotalMinutes < 5; // порог можно менять
        }

        void groupRead_DataChanged2(object subscriptionHandle, object requestHandle, ItemValueResult[] values)
        {
            try
            {
                foreach (ItemValueResult itemValue in values)
                {
                    tegi[itemValue.ItemName] = Convert.ToSingle(itemValue.Value);
                    tegi_kach[itemValue.ItemName] = itemValue.Quality.QualityBits.ToString();
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        void groupRead_DataChanged3(object subscriptionHandle, object requestHandle, ItemValueResult[] values)
        {
            try
            {
                foreach (ItemValueResult itemValue in values)
                {
                    tegi[itemValue.ItemName] = Convert.ToSingle(itemValue.Value);
                    tegi_kach[itemValue.ItemName] = itemValue.Quality.QualityBits.ToString();
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        private void error(string _er)
        {
            try
            {
                Console.WriteLine(_er);
                string t1 = DateTime.Now.ToString("yyyy_MM_dd");
                using (StreamWriter sw = File.AppendText("error_" + t1))
                {
                    string t2 = DateTime.Now.ToString("HH:mm");
                    sw.WriteLine("\r\n" + _er + " в  " + t2);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
        int _ini_table (string name)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection("Data Source=" + "127.0.0.1" + ",1433\\SQLEXPRESS;Initial Catalog=yamid;User ID=klient;Password=1234567;workstation id=(local)WNetSDK;"))

                //using (SqlConnection conn = new SqlConnection("Data Source=(local);Initial Catalog=yamid;Integrated Security=True;Asynchronous Processing=true;"))
                {
                    conn.Open();
                    // First, get schema information of all the tables in current database;
                    DataTable allTablesSchemaTable = conn.GetSchema("Tables");
                    var selectedRows = from info in allTablesSchemaTable.AsEnumerable()
                                       select new
                                       {
                                           TableCatalog = info["TABLE_CATALOG"],
                                           TableSchema = info["TABLE_SCHEMA"],
                                           TableName = info["TABLE_NAME"],
                                       };
                    List<string> _list = new List<string>();
                    foreach (var row in selectedRows)
                    {
                        _list.Add((string)row.TableName);
                    }
                    for (int i = 0; i < _list.Count; i++)
                    {
                        if (_list[i] == name)
                        {
                            return 1;
                        }
                    }
                    return 0;
                }

            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
                return 0;
            }
        }
        // Функция для проверки корректности значений
        private bool IsValidValue(float value, string tagName)
        {
            // Определяем допустимые диапазоны для разных типов параметров
            if (tagName.Contains("Массовый расход"))
            {
                if (value >= 0 && value <= 1500) return true; // Пример диапазона для массового расхода
            }
            else if (tagName.Contains("Температура"))
            {
                if ( value >= -50 && value <= 150) return true; // Пример диапазона для температуры
            }
            else if (tagName.Contains("Давление"))
            {
                if ( value >= 0 && value <= 200) return true; // Пример диапазона для давления
            }
            return false;
        }
        private void усреднение()//добавить фильтр нуля
        {

            try
            {
            
                if (tegi["АртВода.СКМ-2.Текущие параметры.Давление канал 1"] / 1000 >-1 && tegi["АртВода.СКМ-2.Текущие параметры.Давление канал 1"]/1000 <= 1)
                {
                    теги.TryGetValue("АртВода.СКМ-2.Текущие параметры.Давление канал 1", out dann_teg арт_давл);
                    арт_давл.значение += (float)tegi["АртВода.СКМ-2.Текущие параметры.Давление канал 1"]; арт_давл.итераций++;
                    арт_давл.суточное_значение += (float)tegi["АртВода.СКМ-2.Текущие параметры.Давление канал 1"]; арт_давл.суточные_итерации++;
                }
                if (tegi["АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] >= 0 && tegi["АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] <= 1000)
                {
                    теги.TryGetValue("АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1", out dann_teg арт_расход);
                    арт_расход.значение += (float)tegi["АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1"]; арт_расход.итераций++;
                    арт_расход.суточное_значение += (float)tegi["АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1"]; арт_расход.суточные_итерации++;
                }

                if (tegi["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"]/1000 >=-1 && tegi["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"]/1000 <= 1)
                {
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1", out dann_teg хим_давление);
                    хим_давление.значение += (float)tegi["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"]; хим_давление.итераций++;
                    хим_давление.суточное_значение += (float)tegi["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"]; хим_давление.суточные_итерации++;
                }
                if (tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] >= 0 && tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] <= 1000)
                {
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1", out dann_teg хим_расход);
                    хим_расход.значение += (float)tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"]; хим_расход.итераций++;
                    хим_расход.суточное_значение += (float)tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"]; хим_расход.суточные_итерации++;
                }
                if (tegi["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"] >= 0 && tegi["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"] <= 100)
                {
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1", out dann_teg хим_температура);
                    хим_температура.значение += (float)tegi["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"]; хим_температура.итераций++;
                    хим_температура.суточное_значение += (float)tegi["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"]; хим_температура.суточные_итерации++;
                }

                if (tegi["СПСеть.SPT961_1M.т1.156(T)"] >= 0 && tegi["СПСеть.SPT961_1M.т1.156(T)"] <= 300)
                {
                    теги.TryGetValue("СПСеть.SPT961_1M.т1.156(T)", out dann_teg пар_темпер);
                    пар_темпер.значение += (float)tegi["СПСеть.SPT961_1M.т1.156(T)"]; пар_темпер.итераций++;
                    пар_темпер.суточное_значение += (float)tegi["СПСеть.SPT961_1M.т1.156(T)"]; пар_темпер.суточные_итерации++;
                }
                if (tegi["СПСеть.SPT961_1M.т1.154(P)"] >= -1 && tegi["СПСеть.SPT961_1M.т1.154(P)"] <= 1)
                {
                    теги.TryGetValue("СПСеть.SPT961_1M.т1.154(P)", out dann_teg пар_давл);
                    пар_давл.значение += (float)tegi["СПСеть.SPT961_1M.т1.154(P)"]; пар_давл.итераций++;
                    пар_давл.суточное_значение += (float)tegi["СПСеть.SPT961_1M.т1.154(P)"]; пар_давл.суточные_итерации++;
                }
                if (tegi["СПСеть.SPT961_1M.т1.157(G)"] >= 0 && tegi["СПСеть.SPT961_1M.т1.157(G)"] <= 100)
                {
                    теги.TryGetValue("СПСеть.SPT961_1M.т1.157(G)", out dann_teg пар_расх);
                    пар_расх.значение += (float)tegi["СПСеть.SPT961_1M.т1.157(G)"]; пар_расх.итераций++;
                    пар_расх.суточное_значение += (float)tegi["СПСеть.SPT961_1M.т1.157(G)"]; пар_расх.суточные_итерации++;
                }

                //тут необходим фильтр на каждый тег

                foreach (var tag in tegi)
                {
                    if (!IsValidValue(tag.Value, tag.Key))
                    {
                        // Пропускаем некорректное значение
                        continue;
                    }

                    // Здесь обработка корректных значений
                    ProcessData(tag.Key, tag.Value);
                    ProcessDataDen(tag.Key, tag.Value);
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        private bool ProcessTag(string tagKey, ref float targetValue)
        {
            if (tagsDataChas.TryGetValue(tagKey, out var data))
            {
                targetValue = data.Average;
                TagData removedValue;
                tagsDataChas.TryRemove(tagKey, out removedValue);
                return true;
            }
            return false;
        }

        // Метод обработки данных для дневной базы
        private void ProcessDataDen(string tagName, float value)
        {
            // Получаем текущие данные тега или создаем новые
            var currentData = tagsDataDen.GetOrAdd(tagName,
                new TagData { Sum = 0, Count = 0, Quality = "" });

            // Обновляем данные
            currentData.Sum += value;
            currentData.Count++;

            // Сохраняем обновленные данные
            tagsDataDen[tagName] = currentData;
        }

        // Добавляем метод извлечения среднего значения из дневной базы
        private bool ProcessTagDen(string tagKey, ref float targetValue)
        {
            if (tagsDataDen.TryGetValue(tagKey, out var data))
            {
                targetValue = data.Average;
                return true;
            }
            return false;
        }
        private void ProcessData(string tagName, float value)
        {
            // Получаем текущие данные тега или создаем новые
            var currentData = tagsDataChas.GetOrAdd(tagName, new TagData { Sum = 0, Count = 0, Quality = "" });

            // Обновляем данные
            currentData.Sum += value;
            currentData.Count++;

            // Сохраняем обновленные данные
            tagsDataChas[tagName] = currentData;
        }

        private async  void timer1_Tick(object sender, EventArgs e)
            {
            try
            {
                t15_sek++;
                if (t15_sek == 3) t15_sek = 0;
            DisableCloseButton();
            int currentHour   = DateTime.Now.Hour;
            int currentMinute = DateTime.Now.Minute;
            int currentSecond = DateTime.Now.Second;

            // Проверка времени и флага
/*            if (currentHour == 0 && currentMinute == 0 && currentSecond < 11 && statichasResetOccurred==false)
            {
                _reset();
                statichasResetOccurred = true; // Установить флаг, чтобы предотвратить повторный запуск
            }*/
/*            else if (currentHour != 1 || currentMinute != 1 || currentSecond >= 11)
            {
               statichasResetOccurred = false; // Сбросить флаг, если время изменилось
            }*/

                t1_tik++;
                tik_p250m++;
                min_tik++;
                chas_tik++;
                if (min_tik ==720)//раз в час сработка
                {
                    Task t_chas = new Task(() => 
                    {
                        обновить_таблицу_данные_1000("chas");
                        обновить_узлы("chas");
                    });
                    t_chas.Start();
                    Task t_chas_teplos = new Task(() =>
                    {
                        обновить_таблицу_OPC_chas_ХимОчищВода();
                        обновить_таблицу_OPC_chas_АртВода();
                        обновить_таблицу_OPC_chas_Пар();
                        scripts3();
                    });
                    t_chas_teplos.Start();

                    min_tik = 0;
                }
                if (chas_tik == 17280) // раз в сутки сработка
                {
                    timer1.Stop(); // ОСТАНАВЛИВАЕМ таймер, чтобы новые тики не накладывались на запись и перезапуск

                    try
                    {
                        // Спокойно ждем выполнения всех тяжелых функций в фоне
                        await Task.Run(() =>
                        {
                            try { обновить_таблицу_данные_1000("den"); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                            try { обновить_таблицу_OPC_den_ХимОчищВода(); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                            try { обновить_таблицу_OPC_den_АртВода(); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                            try { обновить_таблицу_OPC_den_Пар(); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                            try { scripts_den(); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                            try { обновить_узлы("den"); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                        });

                        // Сюда код дойдет, только когда ВСЕ функции выше завершатся успешно.
                        // Зацикливание исключено, так как приложение полностью закрывается.
                        Application.Restart();
                        Environment.Exit(0);
                    }
                    catch (Exception ex)
                    {
                        // Если произошла непредвиденная критическая ошибка самого await
                        error("Ошибка при выполнении суточного таска: " + ex.Message);

                        // Включаем таймер обратно, чтобы приложение не зависло «намертво»
                        chas_tik = 0;
                        timer1.Start();
                    }
                }


                /*                if (chas_tik == 17280)//раз в сутки сработка
                                {
                                    Task t_den = new Task(() =>
                                    {
                                        try { обновить_таблицу_данные_1000("den"); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                                        try { обновить_таблицу_OPC_den_ХимОчищВода(); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                                        try { обновить_таблицу_OPC_den_АртВода(); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                                        try { обновить_таблицу_OPC_den_Пар(); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                                        try { scripts_den(); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                                        try { обновить_узлы("den"); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
                                    });
                                    t_den.Start();
                                    chas_tik = 0;
                                }*/
                /*                if (t1_tik == 1)
                                {
                                    Task t0 = new Task(() => { обновить_таблицу_данные_TP2_SHABANI(); });
                                    t0.Start();
                                }*/
                if (t15_sek == 0)
                {
                    Task t1 = new Task(() => { scripts1(); });//teg_onlin>>js
                    t1.Start();
                }
                if (t15_sek == 1)
                {
                    Task t2 = new Task(() => { текущие_значения(); });//teg_online>>mssql
                    t2.Start();
                }
                if (t15_sek == 2)
                {
                    Task t3 = new Task(() => { scripts2(); });//арт хим вода 10_min таблица>>js
                    t3.Start();
                }

                if (t1_tik >= 120)
                {
                    Task t4 = new Task(() => { обновить_таблицу_данные_10_min(); });//Тех вода/Арт/Хим >>mssql
                    t4.Start();
                    t1_tik = 0;
                }
                if (t1_tik == 2)
                {
                    Task t5 = new Task(() => { scripts4(); });//Тех вода таблица >>js
                    t5.Start();
                }

                if (tik_p250m == 12)
                {
                        Task t6 = new Task(() => { scripts5("p_250M_data"); });//p_250m >>js
                        t6.Start();
                        tik_p250m = 0;
                }
                if (tik_p250m == 11)
                {
                    string база_вчера = "p_250M_data_" + DateTime.Now.AddDays(-1).ToString("yyyy_MM_dd");
                    string[] allfiles = Directory.GetFiles(@"C:\node_js\baza");
                    for (int i = 0; i < allfiles.Length; i++)
                    {
                        allfiles[i] = allfiles[i].Remove(allfiles[i].Length - 5, 5);
                        allfiles[i] = allfiles[i].Remove(0, 16);
                    }
                    bool совподение = false;
                    for (int i = 0; i < allfiles.Length; i++)
                    {
                        совподение = false;
                        if (allfiles[i] == база_вчера) совподение =true;
                    }
                    if (совподение == false)   
                    { 
                        Task t7 = new Task(() => { scripts5(база_вчера); });
                        t7.Start();
                    }

                }
                if (t1_tik ==3)//раз в 10 мин шабана ТП2 ПАР обновить данные 
                {
                    Task t8 = new Task(() => { обновить_таблицу_данные_1000("data"); });
                    t8.Start();
                }
                if (t1_tik == 4)
                {
                    Task t9 = new Task(() => { scripts_par(); });
                    t9.Start();
                }
                if (t1_tik == 5)
                {
                    Task t10 = new Task(() => { scripts_all(); });
                    t10.Start();
                }
                if (t1_tik == 6)
                {
                    Task t11 = new Task(() => {обновить_узлы("data");});
                    t11.Start();
                }
                if (checkBox2.Checked)
                {
                     textBox1.Text = "";
                     for (int i = 0; i < tegi.Count; i++)
                     {
                         textBox1.Text += tegi.Keys.ElementAt(i) + "=" + tegi[tegi.Keys.ElementAt(i)] + " Качество сигнала=" + tegi_kach[tegi.Keys.ElementAt(i)] + "\r\n";
                     }
                }

                усреднение();
                rabota++;
                if (rabota == 6561) rabota = 0;
                запись_в_файл_для_проверки_роботоспособности(rabota);
                label1.Text = "Осталось до записи в дневные = "+(17280 - chas_tik).ToString();
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }

        private static void запись_в_файл_для_проверки_роботоспособности(int dann)
        {
            using (StreamWriter sw = new StreamWriter(@"c:\serv_askut_2\kontrol_rab"))
            {
                sw.WriteLine(dann.ToString());
            }
        }
/*        private void обновить_АртХимПар_час()
        {
            string data = DateTime.Now.ToString("yyyy_MM_dd");
            string timeSaved = DateTime.Now.ToString(" H:mm:ss ");

            using (SqlConnection connection = new SqlConnection(server_name))
            {
                SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_ХимОчищВода", connection);
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable dt = ds.Tables[0];
                Single _M1 = tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"];
                Single _p1 = tegi["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"];
                Single Δm = tegi["ХимОчищВода.СКМ-2.Текущие параметры.Объем канал 1"];
                Single T1 = tegi["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"];
                for (int z = 1; z < 1000; z++)
                {
                    dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                    dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                    dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                    dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                    dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                    dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                    dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                    dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                }
                dt.Rows[999]["Data"] = data + timeSaved;
                dt.Rows[999]["Δm"] = Δm;
                dt.Rows[999]["M1"] = _M1;
                dt.Rows[999]["p1"] = Math.Round(_p1, 3);
                dt.Rows[999]["t1"] = T1;
                SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                adapter.Update(ds);
                ds.Clear();
                // перезагружаем данные
                adapter.Fill(ds);
            }
        }*/
        private void обновить_таблицу_OPC_den_ХимОчищВода()
        {
            //сместить на одну строчку mssql 
            //обновить JS
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_den_ХимОчищВода", connection);

                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _M1 = 0;
                    Single _p1 = 0;
                    Single T1 = 0;
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1", out dann_teg хим_расх);
                    _M1 = хим_расх.суточное_значение / хим_расх.суточные_итерации; хим_расх.суточное_значение = 0; хим_расх.суточные_итерации = 0;
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1", out dann_teg хим_давл);
                    _p1 = хим_давл.суточное_значение / хим_давл.суточные_итерации / 1000; хим_давл.суточное_значение = 0; хим_давл.суточные_итерации = 0;
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1", out dann_teg хим_темп);
                    T1 = хим_темп.суточное_значение / хим_темп.суточные_итерации; хим_темп.суточное_значение = 0; хим_темп.суточные_итерации = 0;


                    float value = 0;
                    for (int i = 0; i < 3; i++)
                    {
                        if (i == 0) value = _M1;
                        if (i == 1) value = _p1;
                        if (i == 2) value = T1;
                        // Проверка на NaN и Infinity
                        if (float.IsNaN(value) || float.IsInfinity(value))
                        {
                            throw new ArgumentException("Значение не может быть NaN или Infinity.");
                        }
                    }

                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = 0;
                    dt.Rows[999]["M1"] = (float)Math.Round(_M1, 2);
                    dt.Rows[999]["p1"] = (float)Math.Round(_p1, 2);
                    dt.Rows[999]["t1"] = (float)Math.Round(T1, 0);
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        private void обновить_таблицу_OPC_den_АртВода()
        {
            //сместить на одну строчку mssql 
            //обновить JS
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_den_АртВода", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _M1 = 0;
                    Single _p1 = 0;
                    теги.TryGetValue("АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1", out dann_teg арт_расход);
                    _M1 = арт_расход.суточное_значение / арт_расход.суточные_итерации; арт_расход.суточные_итерации = 0; арт_расход.суточное_значение = 0;

                    теги.TryGetValue("АртВода.СКМ-2.Текущие параметры.Давление канал 1", out dann_teg арт_давл);
                    _p1 = (арт_давл.суточное_значение / арт_давл.суточные_итерации) / 1000; арт_давл.суточные_итерации = 0; арт_давл.суточное_значение = 0;


                    float value = 0;
                    for (int i = 0; i < 2; i++)
                    {
                        if (i == 0) value = _M1;
                        if (i == 1) value = _p1;
                        // Проверка на NaN и Infinity
                        if (float.IsNaN(value) || float.IsInfinity(value))
                        {
                            throw new ArgumentException("Значение не может быть NaN или Infinity.");
                        }
                    }
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = 0;
                    dt.Rows[999]["M1"] = (float)Math.Round(_M1, 2);
                    dt.Rows[999]["p1"] = (float)Math.Round(_p1, 2);
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }


            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        private void обновить_таблицу_OPC_den_Пар()
        {
            //сместить на одну строчку mssql 
            //обновить JS
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_den_teplos_par", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _T = 0;
                    Single _P = 0;
                    Single _G = 0;
                    float value = 0;
                    for (int i = 0; i < 3; i++)
                    {
                        if (i == 0) value = _T;
                        if (i == 1) value = _P;
                        if (i == 2) value = _G;
                        // Проверка на NaN и Infinity
                        if (float.IsNaN(value) || float.IsInfinity(value))
                        {
                            throw new ArgumentException("Значение не может быть NaN или Infinity.");
                        }
                    }

                    теги.TryGetValue("СПСеть.SPT961_1M.т1.156(T)", out dann_teg пар_T);
                    _T = пар_T.суточное_значение / пар_T.суточные_итерации; пар_T.суточные_итерации = 0; пар_T.суточное_значение = 0;
                    теги.TryGetValue("СПСеть.SPT961_1M.т1.154(P)", out dann_teg пар_P);
                    _P = пар_P.суточное_значение / пар_P.суточные_итерации; пар_P.суточные_итерации = 0; пар_P.суточное_значение = 0;
                    теги.TryGetValue("СПСеть.SPT961_1M.т1.157(G)", out dann_teg пар_G);
                    _G = пар_G.суточное_значение / пар_G.суточные_итерации; пар_G.суточные_итерации = 0; пар_G.суточное_значение = 0;


                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["time"] = dt.Rows[z]["time"];
                        dt.Rows[z - 1]["T"] = dt.Rows[z]["T"];
                        dt.Rows[z - 1]["P"] = dt.Rows[z]["P"];
                        dt.Rows[z - 1]["G"] = dt.Rows[z]["G"];
                    }
                    dt.Rows[999]["time"] = data + timeSaved;
                    dt.Rows[999]["T"] = (float)Math.Round(_T, 2);
                    dt.Rows[999]["P"] = (float)Math.Round(_P, 2);
                    dt.Rows[999]["G"] = (float)Math.Round(_G, 2);
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }

            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        private void обновить_таблицу_OPC_chas_ХимОчищВода()
        {
            //сместить на одну строчку mssql 
            //обновить JS
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_chas_ХимОчищВода", connection);

                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _M1 = 0;
                    Single _p1 = 0;
                    Single T1 = 0;
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1", out dann_teg хим_расх);
                    _M1 = хим_расх.значение / хим_расх.итераций; хим_расх.значение = 0; хим_расх.итераций = 0;
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1", out dann_teg хим_давл);
                    _p1 = хим_давл.значение / хим_давл.итераций / 1000; хим_давл.итераций = 0; хим_давл.значение = 0;
                    теги.TryGetValue("ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1", out dann_teg хим_темп);
                    T1 = хим_темп.значение / хим_темп.итераций; хим_темп.итераций = 0; хим_темп.значение = 0;


                    float value = 0;
                    for (int i = 0; i < 3; i++)
                    {
                        if (i == 0) value = _M1;
                        if (i == 1) value = _p1;
                        if (i == 2) value = T1;
                        // Проверка на NaN и Infinity
                        if (float.IsNaN(value) || float.IsInfinity(value))
                        {
                            throw new ArgumentException("Значение не может быть NaN или Infinity.");
                        }
                    }

                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = 0;
                    dt.Rows[999]["M1"] = (float)Math.Round(_M1, 2);
                    dt.Rows[999]["p1"] = (float)Math.Round(_p1, 2);
                    dt.Rows[999]["t1"] = (float)Math.Round(T1, 0);
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        private void обновить_таблицу_OPC_chas_АртВода()
        {
            //сместить на одну строчку mssql 
            //обновить JS
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_chas_АртВода", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _M1 = 0;
                    Single _p1 = 0;
                    теги.TryGetValue("АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1", out dann_teg арт_расход);
                    _M1 = арт_расход.значение / арт_расход.итераций; арт_расход.итераций = 0; арт_расход.значение = 0;

                    теги.TryGetValue("АртВода.СКМ-2.Текущие параметры.Давление канал 1", out dann_teg арт_давл);
                    _p1 = (арт_давл.значение / арт_давл.итераций) / 1000; арт_давл.итераций = 0; арт_давл.значение = 0;


                    float value = 0;
                    for (int i = 0; i < 2; i++)
                    {
                        if (i == 0) value = _M1;
                        if (i == 1) value = _p1;
                        // Проверка на NaN и Infinity
                        if (float.IsNaN(value) || float.IsInfinity(value))
                        {
                            throw new ArgumentException("Значение не может быть NaN или Infinity.");
                        }
                    }
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = 0;
                    dt.Rows[999]["M1"] = (float)Math.Round(_M1, 2);
                    dt.Rows[999]["p1"] = (float)Math.Round(_p1, 2);
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }


            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        private void обновить_таблицу_OPC_chas_Пар()
        {
            //сместить на одну строчку mssql 
            //обновить JS
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_chas_teplos_par", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _T = 0;
                    Single _P = 0;
                    Single _G = 0;
                    float value = 0;
                    for (int i = 0; i < 3; i++)
                    {
                        if (i == 0) value = _T;
                        if (i == 1) value = _P;
                        if (i == 2) value = _G;
                        // Проверка на NaN и Infinity
                        if (float.IsNaN(value) || float.IsInfinity(value))
                        {
                            throw new ArgumentException("Значение не может быть NaN или Infinity.");
                        }
                    }

                    теги.TryGetValue("СПСеть.SPT961_1M.т1.156(T)", out dann_teg пар_T);
                    _T = пар_T.значение / пар_T.итераций; пар_T.итераций = 0; пар_T.значение = 0;
                    теги.TryGetValue("СПСеть.SPT961_1M.т1.154(P)", out dann_teg пар_P);
                    _P = пар_P.значение / пар_P.итераций; пар_P.итераций = 0; пар_P.значение = 0;
                    теги.TryGetValue("СПСеть.SPT961_1M.т1.157(G)", out dann_teg пар_G);
                    _G = пар_G.значение / пар_G.итераций; пар_G.итераций = 0; пар_G.значение = 0;


                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["time"] = dt.Rows[z]["time"];
                        dt.Rows[z - 1]["T"] = dt.Rows[z]["T"];
                        dt.Rows[z - 1]["P"] = dt.Rows[z]["P"];
                        dt.Rows[z - 1]["G"] = dt.Rows[z]["G"];
                    }
                    dt.Rows[999]["time"] = data + timeSaved;
                    dt.Rows[999]["T"] = (float)Math.Round(_T, 2);
                    dt.Rows[999]["P"] = (float)Math.Round(_P, 2);
                    dt.Rows[999]["G"] = (float)Math.Round(_G, 2);
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }

            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        /*
                private void обновить_таблицу_OPC_chas_АртВода()
                {
                    //сместить на одну строчку mssql 
                    //обновить JS
                    try
                    {
                        string data = DateTime.Now.ToString("yyyy_MM_dd");
                        string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                        using (SqlConnection connection = new SqlConnection(server_name))
                        {
                            SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_chas_АртВода", connection);
                            DataSet ds = new DataSet();
                            adapter.Fill(ds);
                            DataTable dt = ds.Tables[0];
                            Single _M1 = 0;
                            Single _p1 = 0;
                            теги.TryGetValue("АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1", out dann_teg арт_расход);
                            _M1 = арт_расход.значение / арт_расход.итераций; арт_расход.итераций = 0; арт_расход.значение = 0;

                            теги.TryGetValue("АртВода.СКМ-2.Текущие параметры.Давление канал 1", out dann_teg арт_давл);
                            _p1 = (арт_давл.значение / арт_давл.итераций) / 1000; арт_давл.итераций = 0; арт_давл.значение = 0;


                            float value = 0;
                            for (int i = 0; i < 2; i++)
                            {
                                if (i == 0) value = _M1;
                                if (i == 1) value = _p1;
                                // Проверка на NaN и Infinity
                                if (float.IsNaN(value) || float.IsInfinity(value))
                                {
                                    throw new ArgumentException("Значение не может быть NaN или Infinity.");
                                }
                            }
                            for (int z = 1; z < 1000; z++)
                            {
                                dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                                dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                                dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                                dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                                dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                                dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                                dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                                dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                            }
                            dt.Rows[999]["Data"] = data + timeSaved;
                            dt.Rows[999]["Δm"] = 0;
                            dt.Rows[999]["M1"] = (float)Math.Round(_M1, 2); 
                            dt.Rows[999]["p1"] = (float)Math.Round(_p1, 2); 
                            SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                            adapter.Update(ds);
                            ds.Clear();
                            // перезагружаем данные
                            adapter.Fill(ds);
                        }


                    }
                    catch (Exception ex)
                    {
                        error(ex.Message + ex.StackTrace);
                    }
                }*/

        private void обновить_узлы(string база)
        {
            обновить_узел("OPC_"+база+"_Рамный1", "Рамный1", база);
            обновить_узел("OPC_"+база+"_Рамный2", "Рамный2", база);
            обновить_узел("OPC_"+база+"_Нормали", "Нормали", база);
            обновить_узел("OPC_"+база+"_ЦПО", "ЦПО", база);
            обновить_узел("OPC_"+база+"_МСЦ_2", "МСЦ-2", база);
            обновить_узел("OPC_"+база+"_Агрегатный", "Агрегатный", база);
            обновить_узел("OPC_"+база+"_КЦ", "КЦ", база);
            обновить_узел("OPC_"+база+"_MSK1_TP2", "MSK1 TP2", база);
            обновить_узел("OPC_"+база+"_ЦСиСА", "ЦСиСА", база);//ЦСиСА.СКМ-2.Текущие параметры.Массовый расход канал 1
            обновить_узел("OPC_"+база+"_SLC2_1", "SLC2 1", база);
            обновить_узел("OPC_"+база+"_SLC2_2", "SLC2 2", база);
            обновить_узел("OPC_"+база+"_CPl", "CPl", база);
            обновить_узел("OPC_"+база+"_COM", "COM", база);
            обновить_узел("OPC_"+база+"_CSiOK", "CSiOK", база);
            обновить_узел("OPC_"+база+"_MSK1_SH", "MSK1 SH", база);
            обновить_узел("OPC_"+база+"_КЗЦ", "КЗЦ", база);
            обновить_узел("OPC_"+база+"_PrC", "PrC", база);
            обновить_узел("OPC_"+база+"_ЦМШ", "ЦМШ", база);
            обновить_узел("OPC_"+база+"_ISHZ", "ISHZ", база);
            обновить_узел("OPC_"+база+"_ЭЦ1", "ЭЦ1", база);
            обновить_узел("OPC_"+база+"_CSMA", "CSMA", база);
            обновить_узел("OPC_"+база+"_АТЦ", "АТЦ", база);
            обновить_узел("OPC_"+база+"_CSiSA_24c", "CSiSA 24c", база);
            обновить_узел("OPC_"+база+"_CAA", "CAA", база);

/*            if (база == "chas") count_tegi_chs = 0;
            if (база == "den") count_tegi_den = 0;*/
        }

        private void обновить_узел(string name, string temp, string база)
        {
            try
            {
                Single _M1 = 0, _M2 = 0, _t1 = 0, _t2 = 0, _p1 = 0, _p2 = 0, _delta = 0;

                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                string sql = "SELECT * FROM " + name;
/*              _M1 = tegi[ temp + ".СКМ-2.Текущие параметры.Массовый расход канал 1"];
                _M2 = tegi[ temp + ".СКМ-2.Текущие параметры.Массовый расход канал 2"];
                _t1 = tegi[ temp + ".СКМ-2.Текущие параметры.Температура канала 1"];
                _t2 = tegi[ temp + ".СКМ-2.Текущие параметры.Температура канала 2"];
                _p1 = tegi[ temp + ".СКМ-2.Текущие параметры.Давление канал 1"];
                _p2 = tegi[ temp + ".СКМ-2.Текущие параметры.Давление канал 2"];*/
                if (база == "data")
                {
                if (tegi[temp + ".СКМ-2.Текущие параметры.Массовый расход канал 1"]!=0)
                    _M1 = (Single)tegi[temp + ".СКМ-2.Текущие параметры.Массовый расход канал 1"];
                if (tegi[temp + ".СКМ-2.Текущие параметры.Массовый расход канал 2"] != 0)
                    _M2 = (Single)tegi[temp + ".СКМ-2.Текущие параметры.Массовый расход канал 2"];
                if (tegi[temp + ".СКМ-2.Текущие параметры.Температура канала 1"] != 0)
                    _t1 = (Single)tegi[temp + ".СКМ-2.Текущие параметры.Температура канала 1"];
                if (tegi[temp + ".СКМ-2.Текущие параметры.Температура канала 2"] != 0)
                    _t2 = (Single)tegi[temp + ".СКМ-2.Текущие параметры.Температура канала 2"];
                if (tegi[temp + ".СКМ-2.Текущие параметры.Давление канал 1"] != 0)
                    _p1 = (Single)tegi[temp + ".СКМ-2.Текущие параметры.Давление канал 1"];
                if (tegi[temp + ".СКМ-2.Текущие параметры.Давление канал 2"] != 0)
                    _p2 = (Single)tegi[temp + ".СКМ-2.Текущие параметры.Давление канал 2"];
                    tegi[temp + ".СКМ-2.Текущие параметры.Массовый расход канал 1"] = 0;
                    tegi[temp + ".СКМ-2.Текущие параметры.Массовый расход канал 2"] = 0;
                    tegi[temp + ".СКМ-2.Текущие параметры.Температура канала 1"] = 0;
                    tegi[temp + ".СКМ-2.Текущие параметры.Температура канала 2"] = 0;
                    tegi[temp + ".СКМ-2.Текущие параметры.Давление канал 1"] = 0;
                    tegi[temp + ".СКМ-2.Текущие параметры.Давление канал 2"] = 0;
                    _delta = Math.Abs(_M1 - _M2);
                }
                if (база == "chas")
                {
                    string baseKey = temp + ".СКМ-2.Текущие параметры.";

                    ProcessTag(baseKey + "Массовый расход канал 1", ref _M1);
                    ProcessTag(baseKey + "Массовый расход канал 2", ref _M2);
                    ProcessTag(baseKey + "Температура канала 1", ref _t1);
                    ProcessTag(baseKey + "Температура канала 2", ref _t2);
                    ProcessTag(baseKey + "Давление канал 1", ref _p1);
                    ProcessTag(baseKey + "Давление канал 2", ref _p2);
                    _delta = Math.Abs(_M1 - _M2);
                }
                if (база == "den")
                {
                    string baseKey = temp + ".СКМ-2.Текущие параметры.";

                    ProcessTagDen(baseKey + "Массовый расход канал 1", ref _M1);
                    ProcessTagDen(baseKey + "Массовый расход канал 2", ref _M2);
                    ProcessTagDen(baseKey + "Температура канала 1", ref _t1);
                    ProcessTagDen(baseKey + "Температура канала 2", ref _t2);
                    ProcessTagDen(baseKey + "Давление канал 1", ref _p1);
                    ProcessTagDen(baseKey + "Давление канал 2", ref _p2);
                    _delta = Math.Abs(_M1 - _M2);
                }
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = _delta;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["M2"] = _M2;
                    dt.Rows[999]["t1"] = _t1;
                    dt.Rows[999]["t2"] = _t2;
                    dt.Rows[999]["p1"] = _p1;
                    dt.Rows[999]["p2"] = _p2;
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);

                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace + " " +name + " " + temp);
            }
        }



private void RestartOpcServer()
    {
        // Убедитесь, что сервер запущен
        var processes = Process.GetProcessesByName("das");
        if (processes.Length > 0)
        {
            // Завершите процесс сервера
            foreach (var process in processes)
            {
                process.Kill();
            }
        }

        // Запустите сервер заново
        Process.Start("path/to/das.exe");
    }



    private void f_script(string papka, string name)
        {
            try
            {
                DataTable dt1 = null;
                DataTable dt2 = null;
                DataTable dt3 = null;


                string server_name = "Data Source=192.168.4.138,1433;Initial Catalog=yamid;User ID=klient;Password=1234567;workstation id=(local)WNetSDK,encrypt=false;trustServerCertificate=false;";
                string sql = "SELECT * FROM OPC_data_" + name;
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1 = ds.Tables[0];
                }

                sql = "SELECT * FROM OPC_chas_" + name;
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt2 = ds.Tables[0];
                }

                sql = "SELECT * FROM OPC_den_" + name;
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt3 = ds.Tables[0];
                }
                string js = "";
                string q = "";
                js = "function _10min(){\r\n";
                if (papka=="shabani" || papka =="tp2")
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1.Rows[i]["Data"].ToString() + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt1.Rows[i]["t1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt1.Rows[i]["t2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt1.Rows[i]["p1"]).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt1.Rows[i]["p2"]).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt1.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt1.Rows[i]["M2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_8stolbec').html('" + ((Single)dt1.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    }
                }
                else
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1.Rows[i]["Data"].ToString() + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt1.Rows[i]["t1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt1.Rows[i]["t2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt1.Rows[i]["p1"]/1000).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt1.Rows[i]["p2"]/1000).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt1.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt1.Rows[i]["M2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_8stolbec').html('" + ((Single)dt1.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    }
                }
                js += "};";
                q = @"c:\node_js\yzli\" + papka + "\\" + papka + "_10min.js";
                using (StreamWriter sw = File.CreateText(q))
                {
                    sw.WriteLine(js);
                }
                js = "function chas(){\r\n";
                if (papka == "shabani" || papka == "tp2")
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt2.Rows[i]["Data"].ToString() + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt2.Rows[i]["t1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt2.Rows[i]["t2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt2.Rows[i]["p1"]).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt2.Rows[i]["p2"]).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt2.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt2.Rows[i]["M2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_8stolbec').html('" + ((Single)dt2.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    }
                }
                else
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt2.Rows[i]["Data"].ToString() + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt2.Rows[i]["t1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt2.Rows[i]["t2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt2.Rows[i]["p1"] / 1000).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt2.Rows[i]["p2"] / 1000).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt2.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt2.Rows[i]["M2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_8stolbec').html('" + ((Single)dt2.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    }
                }

                js += "};";
                q = @"c:\node_js\yzli\" + papka + "\\" + papka + "_chas.js";
                if (File.Exists(q))
                    File.Delete(q);
                if (!File.Exists(q))
                    File.Create(q).Close();
                using (var fs = new FileStream(q, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.WriteLine(js); }
                js = "function den(){\r\n";
                if (papka == "shabani" || papka == "tp2")
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt3.Rows[i]["Data"].ToString() + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt3.Rows[i]["t1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt3.Rows[i]["t2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt3.Rows[i]["p1"]).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt3.Rows[i]["p2"]).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt3.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt3.Rows[i]["M2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_8stolbec').html('" + ((Single)dt3.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    }
                }
                else
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt3.Rows[i]["Data"].ToString() + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt3.Rows[i]["t1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt3.Rows[i]["t2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt3.Rows[i]["p1"] / 1000).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt3.Rows[i]["p2"] / 1000).ToString("0.##") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt3.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt3.Rows[i]["M2"]).ToString("0.#") + "');\r\n";
                        js += "$('#" + papka + "_" + (i + 1).ToString() + "stroka_8stolbec').html('" + ((Single)dt3.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    }
                }
                js += "};";
                q = @"c:\node_js\yzli\" + papka + "\\" + papka + "_den.js";
                if (File.Exists(q))
                    File.Delete(q);
                if (!File.Exists(q))
                    File.Create(q).Close();
                using (var fs = new FileStream(q, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.WriteLine(js); }
            }
            catch (Exception ex)
            {
                string t1 = DateTime.Now.ToString("yyyy_MM_dd");
                using (StreamWriter sw = File.AppendText("error_" + t1))
                {
                    string t2 = DateTime.Now.ToString("HH:mm");
                    sw.WriteLine("\r\n" + ex.ToString() + " в  " + t2+" "+papka + " " +name);
                }
                Console.WriteLine(ex);
            }
        }
        private void scripts_all()
        {
            try
            {
                f_script("ramn_1", "Рамный1");
                f_script("ramn_2", "Рамный2");
                f_script("msc2", "МСЦ_2");
                f_script("normali", "Нормали");
                f_script("cpo", "ЦПО");
                f_script("agregatn", "Агрегатный");
                f_script("kc", "КЦ");
                f_script("rc_zaa", "MSK1_TP2");
                f_script("ciisa", "ЦСиСА");
                f_script("slc2_1", "SLC2_1");
                f_script("slc2_2", "SLC2_2");
                f_script("cpl", "CPl");
                f_script("com", "COM");
                f_script("csiok", "CSiOK");
                f_script("msk1", "MSK1_SH");
                f_script("kzc", "КЗЦ");
                f_script("prc", "PrC");
                f_script("cmh", "ЦМШ");
                f_script("ihz", "ISHZ");
                f_script("ec1", "ЭЦ1");
                f_script("24c_boxi", "CSMA");
                f_script("atc", "АТЦ");
                f_script("maz_24", "CSiSA_24c");
                f_script("CAA", "CAA");
                f_script("tp2", "tp2");
                f_script("shabani", "SHABANI");
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        void обновить_таблицу_данные_TP2_SHABANI()
        {
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                Single _M1, _M2, _t1, _t2, _p1, _p2, _delta;
                _M1 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/q"];
                _M2 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/q"];
                _t1 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/T"];
                _t2 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/T"];
                _p1 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/P"];
                _p2 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/P"];

                _delta = Math.Abs(_M1 - _M2);
                string sql = "SELECT * FROM OPC_data_SHABANI";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = _delta;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["M2"] = _M2;
                    dt.Rows[999]["t1"] = _t1;
                    dt.Rows[999]["t2"] = _t2;
                    dt.Rows[999]["p1"] = _p1;
                    dt.Rows[999]["p2"] = _p2;


                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
                _M1 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/q"] ;
                _M2 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/q"] ;
                _t1 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/T"] ;
                _t2 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/T"] ;
                _p1 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/P"] ;
                _p2 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/P"] ;

                _delta = Math.Abs(_M1 - _M2);
                sql = "SELECT * FROM OPC_chas_TP2";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = _delta;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["M2"] = _M2;
                    dt.Rows[999]["t1"] = _t1;
                    dt.Rows[999]["t2"] = _t2;
                    dt.Rows[999]["p1"] = _p1;
                    dt.Rows[999]["p2"] = _p2;


                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        void обновить_таблицу_данные_1000(string name)
        {
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                Single _M1, _M2, _t1, _t2, _p1, _p2, _delta;
                _M1 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/q"]/1000;
                _M2 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000;
                _t1 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/T"];
                _t2 = tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/T"];
                _p1 = (tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/P"]-100)/1000;
                _p2 = (tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/P"]-100)/1000;
                _delta = Math.Abs(_M1 - _M2);
                string sql = "SELECT * FROM OPC_" + name + "_SHABANI";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = _delta;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["M2"] = _M2;
                    dt.Rows[999]["t1"] = _t1;
                    dt.Rows[999]["t2"] = _t2;
                    dt.Rows[999]["p1"] = _p1;
                    dt.Rows[999]["p2"] = _p2;


                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
                _M1 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/q"]/1000;
                _M2 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000;
                _t1 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/T"];
                _t2 = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/T"];
                _p1 = (tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/P"]-100)/1000;
                _p2 = (tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/P"] - 100)/1000;
                _delta = Math.Abs(_M1 - _M2);
                sql = "SELECT * FROM OPC_" + name + "_TP2";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = _delta;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["M2"] = _M2;
                    dt.Rows[999]["t1"] = _t1;
                    dt.Rows[999]["t2"] = _t2;
                    dt.Rows[999]["p1"] = _p1;
                    dt.Rows[999]["p2"] = _p2;


                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
                _t1 = tegi["PAR/CurrentValues/Pipeline/Pc1/T"];
                _p1 = tegi["PAR/CurrentValues/Pipeline/Pc1/P"]/1000;
                _M1 = tegi["PAR/CurrentValues/Pipeline/Pc1/q"]/1000;
                _delta = tegi["PAR/Counters/Pipeline/Pc1/q"]/1000;

                sql = "SELECT * FROM OPC_" + name + "_PAR";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["time"] = dt.Rows[z]["time"];
                        dt.Rows[z - 1]["T"] = dt.Rows[z]["T"];
                        dt.Rows[z - 1]["P"] = dt.Rows[z]["P"];
                        dt.Rows[z - 1]["G"] = dt.Rows[z]["G"];
                    }
                    dt.Rows[999]["time"] = data + timeSaved;
                    dt.Rows[999]["T"] = _t1;
                    dt.Rows[999]["P"] = _p1;
                    dt.Rows[999]["G"] = _M1;


                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }

        }
        private void scripts_par()
        {
            DataTable dt1 = null;
            DataTable dt2 = null;
            DataTable dt3 = null;
            string server_name = "Data Source=192.168.4.138,1433;Initial Catalog=yamid;User ID=klient;Password=1234567;workstation id=(local)WNetSDK,encrypt=false;trustServerCertificate=false;";
            string sql = "SELECT * FROM OPC_data_PAR";
            using (SqlConnection connection = new SqlConnection(server_name))
            {
                // Создаем объект DataAdapter
                SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                // Создаем объект Dataset
                DataSet ds = new DataSet();
                // Заполняем Dataset
                adapter.Fill(ds);
                dt1 = ds.Tables[0];
            }
            sql = "SELECT * FROM OPC_chas_PAR";
            using (SqlConnection connection = new SqlConnection(server_name))
            {
                // Создаем объект DataAdapter
                SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                // Создаем объект Dataset
                DataSet ds = new DataSet();
                // Заполняем Dataset
                adapter.Fill(ds);
                dt2 = ds.Tables[0];
            }
            sql = "SELECT * FROM OPC_den_PAR";
            using (SqlConnection connection = new SqlConnection(server_name))
            {
                // Создаем объект DataAdapter
                SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                // Создаем объект Dataset
                DataSet ds = new DataSet();
                // Заполняем Dataset
                adapter.Fill(ds);
                dt3 = ds.Tables[0];
            }
            string js = "$(document).ready(function(){\r\n";
            for (int i = 0; i < 1000; i++)
            {
                js += "$('#par_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1.Rows[i]["time"].ToString() + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_2stolbec').html('" + Math.Round((Single)dt1.Rows[i]["T"], 2) + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_3stolbec').html('" + Math.Round((Single)dt1.Rows[i]["P"], 2) + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_4stolbec').html('" + Math.Round((Single)dt1.Rows[i]["G"], 2) + "');\r\n";
            }
            js += "});";
            using (StreamWriter sw = File.CreateText(@"c:\node_js\javascript\par_komerch\table_par_1.js"))
            {
                sw.WriteLine(js);
            }
            js = "function chas(){\r\n";
            for (int i = 0; i < 1000; i++)
            {
                js += "$('#par_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt2.Rows[i]["time"].ToString() + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_2stolbec').html('" + Math.Round((Single)dt2.Rows[i]["T"], 2) + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_3stolbec').html('" + Math.Round((Single)dt2.Rows[i]["P"], 2) + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_4stolbec').html('" + Math.Round((Single)dt2.Rows[i]["G"], 2) + "');\r\n";
            }
            js += "};";
            using (StreamWriter sw = File.CreateText(@"c:\node_js\javascript\par_komerch\table_par_2.js"))
            {
                sw.WriteLine(js);
            }
            js = "function den(){\r\n";
            for (int i = 0; i < 1000; i++)
            {
                js += "$('#par_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt3.Rows[i]["time"].ToString() + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_2stolbec').html('" + Math.Round((Single)dt3.Rows[i]["T"], 2) + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_3stolbec').html('" + Math.Round((Single)dt3.Rows[i]["P"], 2) + "');\r\n";
                js += "$('#par_" + (i + 1).ToString() + "stroka_4stolbec').html('" + Math.Round((Single)dt3.Rows[i]["G"], 2) + "');\r\n";
            }
            js += "};";
            using (StreamWriter sw = File.CreateText(@"c:\node_js\javascript\par_komerch\table_par_3.js"))
            {
                sw.WriteLine(js);
            }
        }
        private void scripts5(string название_базы)
        {
            try
            {
                DataTable dt1 = null;
                string name = "";
                string server_name = "Data Source=192.168.4.138,1433;Initial Catalog=yamid;User ID=klient;Password=1234567;workstation id=(local)WNetSDK,encrypt=false;trustServerCertificate=false;";
                string sql = "SELECT * FROM " + название_базы;
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1 = ds.Tables[0];
                }
                string js = "";
                if (название_базы == "p_250M_data")
                {
                    js = "$(document).ready(function(){\r\n";
                    for (int i = 0; i < 1439; i++)
                    {
                        js += "$('#250M_data" + (i + 1).ToString() + "').html('" + dt1.Rows[i]["data"].ToString() + "');\r\n";
                        js += "$('#250M_val" + (i + 1).ToString() + "').html('" + ((Single)dt1.Rows[i]["val"]).ToString() + "');\r\n";
                    }
                    js += "});";
                    name = @"c:\node_js\javascript\250m.js";
                }
                else
                {
                    string js2 = "";
                    for (int i = 0; i < 1439; i++)
                    {//document.getElementById('foo').textContent = 'Hello, World!'
                        js2 += "document.getElementById('250M_data" + (i + 1).ToString() + "').textContent = '" + dt1.Rows[i]["data"].ToString() + "';";
                        js2 += "document.getElementById('250M_val" + (i + 1).ToString() + "').textContent = '" + ((Single)dt1.Rows[i]["val"]).ToString() + "';";
                    }
                    name = @"c:\node_js\baza\" + название_базы + ".html";
                    js = "<!DOCTYPE html>\r\n<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"ru\" lang=\"ru\">\r\n<head>\r\n<meta charset=\"UTF-8\">\r\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\">\r\n<script src=\"javascript/Frameworks/jquery.min.js\" type=\"text/javascript\"></script>\r\n<script src=\"javascript/highstock.js\"></script>\r\n    <link rel=\"shortcut icon\" href=\"images/favicon.ico\">\r\n    <title>АСКУТ</title>\r\n</head>\r\n" +
                        "<body class=\"styl\">\r\n\t<table id=\"table\">\r\n\t\t<thead>\r\n\t\t\t <tr>\r\n\t\t\t</tr>\r\n\t\t</thead>\r\n</table>\r\n" +
                        "<div id=\"container2\" style=\"height: 35em\" ></div>" +
                        "</body>\r\n" +
                        "<script type = \"text/javascript\">\r\n" +
                        "let table = document.querySelector('#table');\r\n    for (let i = 1; i < 1440; i++) {\r\n    let tr = document.createElement('tr');\r\n    for (let k = 0; k < 2; k++) {\r\n        let td = document.createElement('td');\r\n        td.textContent = '?';\r\n        if (k==0)td.id ='250M_data'+ i.toString();\r\n\t\tif (k==1)td.id ='250M_val'+ i.toString();\r\n\t\ttr.appendChild(td);}\r\ntable.appendChild(tr);}\r\n" +
                        js2 +
                        "var x_data = [];\r\n    var y_val = [];\r\n\tvar arr3 = [];\r\n    for (var i=0;i<1439;i++)\r\n        {\r\n\t\t\tx_data[i] = $('#250M_data'+(i+1).toString()).html();\r\n\t\t\ty_val[i] = $('#250M_val'+(i+1).toString()).html();\r\n        }\r\nvar mas = [2];\r\nfor (var i = 0; i <1438; i++){\r\n\t\tmas[i] = [];\r\n\t\tfor (var j = 0; j < 2; j++){\r\n\t\tvar temp = x_data[i].toString();\r\n\t\tvar temp2 = temp.replace(\".\", \"-\");\r\n\t\ttemp2 = temp2.replace(\".\", \"-\");\r\n\t\tif (j==0) mas[i][j] =temp2;\r\n\t\tif (j==1) mas[i][j] = parseFloat( y_val[i] );\r\n}}\r\nconst _chart2 = Highcharts.chart('container2', {\r\n\tchart: {\r\n            type: 'line',\r\n            zooming: {\r\n            type: 'x',\r\n            singleTouch: true,\r\n            }\r\n      },  \r\n\t      title: {\r\n        text: 'График показаний прибора'\r\n    },      \r\n        xAxis: {\r\n          \ttitle: {\r\n                text: 'Дата и время измерения'\r\n            },\r\n\t\t\ttype: 'category',\r\n        },\r\n\t\tyAxis: {\r\n\t\tmin: 0,\r\n\t\tmax: 7.1,\r\n        title: {\r\n            text: 'Давление (Атм)'}\r\n        },\r\n    scrollbar: {\r\n        enabled: true\r\n    },\r\n\tseries: \r\n\t[{\tanimation: false,\r\n        name: 'P',\r\n        data: mas,\r\n    }]\r\n    });\r\n\r\n" +
                        "</script>\r\n" +
                        "<style>\r\n.container{margin: 0 auto;\r\n       position: absolute;\r\n       border: 3px solid white;\r\n       background-color: #a2a2a2;\r\n       width: 79em;\r\n       height: 41em;\r\n       left: 0px;\r\n       top: 0px;\r\n       user-select:none;}\r\n#p_250M {font-family: Sylfaen;\r\nfont-size: 28px;\r\nline-height: 1.42857143;\r\n}\r\nbody {  background-color:  #000;}\r\n#table{     border: 1px solid white;\r\n            left: 1em;\r\n            top: 3em;\r\n            position: absolute;\r\n            width: 10em;\r\n            overflow-y: scroll;\r\n            height: 23.7em;\r\n            display: table-column;\r\n            font-size: 18pt;}\r\ntr{  height: 1em; }\r\ntd{   width: 5em; text-align: center; }\r\ntr:nth-child(odd) { background: white;}\r\ntr:nth-child(even) { background:  white;}\r\nthead th {\r\n  position: sticky;\r\n  top: 0;\r\n  background: white;\r\n}\r\n#graf{position: absolute;}\r\n#ram1{position: absolute;\r\ntop: 4em;\r\nleft: 18em;\r\nwidth: 63em;}\r\n#container2{ position: absolute;\r\ntop:5em;\r\nleft: 18em;\r\nbackground-color: white;\r\nwidth: 60em;}\r\n#_upd{position: absolute;\r\ntop: 3.5em;\r\nleft: 58em;\r\ncolor: black;}\r\n#calend{position: absolute;\r\n  top: -0.5em;\r\n  left: 56em;\r\n  color: black;}\r\n\r\n</style>\r\n</html>";

                }
                if (File.Exists(name))
                    File.Delete(name);
                if (!File.Exists(name))
                    File.Create(name).Close();
                using (var fs = new FileStream(name, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.Write(js); }
                
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }


        }
        private void scripts4()
        {
            try
            {
                DataTable dt1 = null;
                DataTable dt2 = null;

                DataTable dt1_2 = null;
                DataTable dt2_2 = null;

                DataTable dt1_3 = null;
                DataTable dt2_3 = null;
                string name = "";
                string server_name = "Data Source=192.168.4.138,1433;Initial Catalog=yamid;User ID=klient;Password=1234567;workstation id=(local)WNetSDK,encrypt=false;trustServerCertificate=false;";
                string sql = "SELECT * FROM OPC_data_ТехВода1";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_data_ТехВода2";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt2 = ds.Tables[0];
                }

                sql = "SELECT * FROM OPC_chas_ТехВода1";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1_2 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_chas_ТехВода2";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt2_2 = ds.Tables[0];
                }

                sql = "SELECT * FROM OPC_den_ТехВода1";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1_3 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_den_ТехВода2";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt2_3 = ds.Tables[0];
                }
                string js = "$(document).ready(function(){\r\n";
                for (int i = 0; i < 1000; i++)
                {
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1.Rows[i]["Data"].ToString() + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt1.Rows[i]["p1"] / 1000).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt1.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt1.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt2.Rows[i]["p1"] / 1000).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt2.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt2.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                }
                js += "});";
                name = @"c:\node_js\javascript\teh_voda\table_teh.js";
                if (File.Exists(name))
                    File.Delete(name);
                if (!File.Exists(name))
                    File.Create(name).Close();
                using (var fs = new FileStream(name, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.Write(js); }
                js = "function chas(){\r\n";
                for (int i = 0; i < 1000; i++)
                {
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1_2.Rows[i]["Data"].ToString() + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt1_2.Rows[i]["p1"] / 1000).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt1_2.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt1_2.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt2_2.Rows[i]["p1"] / 1000).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt2_2.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt2_2.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                }
                js += "};";
                name = @"c:\node_js\javascript\teh_voda\table_teh_chas.js";
                if (File.Exists(name))
                    File.Delete(name);
                if (!File.Exists(name))
                    File.Create(name).Close();
                using (var fs = new FileStream(name, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.Write(js); }
                js = "function den(){\r\n";
                for (int i = 0; i < 100; i++)
                {
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1_3.Rows[i]["Data"].ToString() + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_2stolbec').html('" + ((Single)dt1_3.Rows[i]["p1"] / 1000).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_3stolbec').html('" + ((Single)dt1_3.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_4stolbec').html('" + ((Single)dt1_3.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_5stolbec').html('" + ((Single)dt2_3.Rows[i]["p1"] / 1000).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_6stolbec').html('" + ((Single)dt2_3.Rows[i]["M1"]).ToString("0.#") + "');\r\n";
                    js += "$('#teh_" + (i + 1).ToString() + "stroka_7stolbec').html('" + ((Single)dt2_3.Rows[i]["Δm"]).ToString("0.#") + "');\r\n";
                }
                js += "};";
                name = @"c:\node_js\javascript\teh_voda\table_teh_den.js";
                if (File.Exists(name))
                    File.Delete(name);
                if (!File.Exists(name))
                    File.Create(name).Close();
                using (var fs = new FileStream(name, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.Write(js); }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }


        }
        private void scripts1()
        {
            try
            {
/*              tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/P"]  = tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/P"]  - 0.1f;
                tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/P"]  = tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/P"]  - 0.1f;
                tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/P"] = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/P"] - 0.1f;
                tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/P"] = tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/P"] - 0.1f;*/
                int sec1 = Convert.ToInt32(tegi["ТехВода1.СКМ-2.Group1.Tag1"]);
                int minutes1 = sec1 / 60;
                int newSec1 = sec1 - minutes1 * 60;
                int hour1 = minutes1 / 60;
                int newMinnutes1 = minutes1 - hour1 * 60;

                int sec2 = Convert.ToInt32(tegi["ТехВода2.СКМ-2.Group1.Tag1"]);
                int minutes2 = sec2 / 60;
                int newSec2 = sec2 - minutes2 * 60;
                int hour2 = minutes2 / 60;
                int newMinnutes2 = minutes2 - hour2 * 60;
                string t1 = DateTime.Now.ToString("yyyy-MM-dd HH:mm");
                //TimeSpan TS = new TimeSpan(hour, newMinnutes, newSec);
                string js = "";
                js += "$(document).ready(function() {\r\n";
                js += "$('#_date_upd').text('" + "Данные на: " + t1 + "');\r\n";
                if (p_250M !=200000) js += "$('#p_250M').text('" + "ДИСК 250М,Показания прибора "+data_p_250m+" P="+p_250M + "');\r\n";
                if (p_250M == 200000) js += "$('#p_250M').text('" + "ДИСК 250М,Показания прибора: Нет связи!!!" + "');\r\n";
                string _tag_ = "СПСеть.SPT961_1M.т1.156(T)";
                js += "$('#p_t').text('" + tegi[_tag_].ToString("0.##") + "');\r\n";
                DateTime lastUpdate = new DateTime(1970, 1, 1).AddSeconds(tegi_lastUnix[_tag_]); 
                string formattedTime = lastUpdate.ToString("yyyy-MM-dd HH:mm:ss");
                js += "$('#p_t').attr('title', 'Последнее изменение: " + formattedTime + "');\r\n";
                js += "$('#p_p').text('" + tegi["СПСеть.SPT961_1M.т1.154(P)"].ToString("0.##") + "');\r\n";
                js += "$('#p_g').text('" + tegi["СПСеть.SPT961_1M.т1.157(G)"].ToString("0.##") + "');\r\n";
                js += "$('#p_w').text('" + tegi["СПСеть.SPT961_1M.т1.158(w)"].ToString("0.##") + "');\r\n";
                js += "$('#p_v').text('" + tegi["СПСеть.SPT961_1M.т1.163(Vo)"].ToString("0.##") + "');\r\n";
                js += "$('#p_Ws').text('" + tegi["СПСеть.SPT961_1M.т1.161(Ws)"].ToString("0.##") + "');\r\n";
                if (tegi_kach["СПСеть.SPT961_1M.т1.156(T)"] != "good") js += "$('#p_t').css('background', 'gray');\r\n";
                else js += "$('#p_t').css('background', 'white');\r\n";
                if (tegi_kach["СПСеть.SPT961_1M.т1.154(P)"] != "good") js += "$('#p_p').css('background', 'gray');\r\n";
                else js += "$('#p_p').css('background', 'white');\r\n";
                if (tegi_kach["СПСеть.SPT961_1M.т1.157(G)"] != "good") js += "$('#p_g').css('background', 'gray');\r\n";
                else js += "$('#p_g').css('background', 'white');\r\n";
                if (tegi_kach["СПСеть.SPT961_1M.т1.158(w)"] != "good") js += "$('#p_w').css('background', 'gray');\r\n";
                else js += "$('#p_w').css('background', 'white');\r\n";
                if (tegi_kach["СПСеть.SPT961_1M.т1.163(Vo)"] != "good") js += "$('#p_v').css('background', 'gray');\r\n";
                else js += "$('#p_v').css('background', 'white');\r\n";
                if (tegi_kach["СПСеть.SPT961_1M.т1.161(Ws)"] != "good") js += "$('#p_Ws').css('background', 'gray');\r\n";
                else js += "$('#p_Ws').css('background', 'white');\r\n";
                js += "$('#a_p').text('" + ((Single)tegi["АртВода.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#a_q').text('" + tegi["АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#a_v').text('" + tegi["АртВода.СКМ-2.Текущие параметры.Объем канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#x_t').text('" + tegi["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#x_p').text('" + ((Single)tegi["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#x_v').text('" + tegi["ХимОчищВода.СКМ-2.Текущие параметры.Объем канал 1"].ToString("0.##") + "');\r\n";
                if (tegi_kach["АртВода.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#a_p').css('background', 'gray');\r\n";
                else js += "$('#a_p').css('background', 'white');\r\n";
                if (tegi_kach["АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#a_q').css('background', 'gray');\r\n";
                else js += "$('#a_q').css('background', 'white');\r\n";
                if (tegi_kach["АртВода.СКМ-2.Текущие параметры.Объем канал 1"] != "good") js += "$('#a_v').css('background', 'gray');\r\n";
                else js += "$('#a_v').css('background', 'white');\r\n";
                if (tegi_kach["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"] != "good")
                {
                    js += "$('#x_t').css('background', 'gray');\r\n";
                    js += "$('#x_t').text('???');\r\n";
                }
                else
                {
                    js += "const temperatureDiv = document.getElementById('x_t');\r\n";
                    js += "var temp = $('#x_t').html();\r\n";
                    js += "if (temp < 60) { $('#x_t').css('background', 'white' ); temperatureDiv.classList.remove('pulsate');  };\r\n";
                    js += "if (temp >= 60) { $('#x_t').css('background', 'yellow' ); temperatureDiv.classList.add('pulsate');  };\r\n";
                    js += "if (temp >= 65) { $('#x_t').css('background', 'red' );  audio.play();  };\r\n";
                }
                if (tegi_kach["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"] != "good")
                {
                    js += "$('#x_p').css('background', 'gray');\r\n";
                    js += "$('#x_p').text('???');\r\n";
                }
                else js += "$('#x_p').css('background', 'white');\r\n";
                if (tegi_kach["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good")
                {
                    js += "$('#x_q').css('background', 'gray');\r\n";
                    js += "$('#x_q').text('???');\r\n";
                }
                else
                {
                    js += "$('#x_q').css('background', 'white');\r\n";
                    if (tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] > 100) js += "$('#x_q').css('background', 'yellow');\r\n";
                    if (tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"] > 100) js += "$('#x_q').css('background', 'white');\r\n";
                    js += "$('#x_q').text('" + tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                }
                if (tegi_kach["ХимОчищВода.СКМ-2.Текущие параметры.Объем канал 1"] != "good")
                {
                    js += "$('#x_v').css('background', 'gray');\r\n";
                    js += "$('#x_v').text('???');\r\n";
                }
                else js += "$('#x_v').css('background', 'white');\r\n";
                //if (_ap>=0.05) $('#a_p').css( \"background\", \"white\" );
                //if (_ap<0.05)  $('#a_p').css( \"background\", \"red\" );
                
                js += "$('#par_t').text('" + tegi["PAR/CurrentValues/Pipeline/Pc1/T"].ToString("0.##") + "');\r\n";
                js += "$('#par_p').text('" + (tegi["PAR/CurrentValues/Pipeline/Pc1/P"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#par_g').text('" + (tegi["PAR/CurrentValues/Pipeline/Pc1/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#par_ws').text('" + (tegi["PAR/Counters/Pipeline/Pc1/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#par_qs').text('" + (tegi["PAR/Counters/Pipeline/Pc1/W"] / 1000000).ToString("0.##") + "');\r\n";
                if (tegi_kach["PAR/CurrentValues/Pipeline/Pc1/T"] != "good") js += "$('#par_t').css('background', 'gray');\r\n";
                else js += "$('#par_t').css('background', 'white');\r\n";
                if (tegi_kach["PAR/CurrentValues/Pipeline/Pc1/P"] != "good") js += "$('#par_p').css('background', 'gray');\r\n";
                else js += "$('#par_p').css('background', 'white');\r\n";
                if (tegi_kach["PAR/CurrentValues/Pipeline/Pc1/q"] != "good") js += "$('#par_g').css('background', 'gray');\r\n";
                else js += "$('#par_g').css('background', 'white');\r\n";
                if (tegi_kach["PAR/Counters/Pipeline/Pc1/q"] != "good") js += "$('#par_ws').css('background', 'gray');\r\n";
                else js += "$('#par_ws').css('background', 'white');\r\n";

                if (tegi_kach["PAR/Counters/Pipeline/Pc1/W"] != "good") js += "$('#par_qs').css('background', 'gray');\r\n";
                else js += "$('#par_qs').css('background', 'white');\r\n";

                js += "$('#t1_p').text('" + ((Single)tegi["ТехВода1.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_q').text('" + tegi["ТехВода1.СКМ-2.Текущие параметры.Объемный расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_v').text('" + tegi["ТехВода1.СКМ-2.Текущие параметры.Объем канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_vrem').text('" + hour1 + ":" + newMinnutes1 + ":" + newSec1 + "');\r\n";
                js += "$('#t2_p').text('" + ((Single)tegi["ТехВода2.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_q').text('" + tegi["ТехВода2.СКМ-2.Текущие параметры.Объемный расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_v').text('" + tegi["ТехВода2.СКМ-2.Текущие параметры.Объем канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_vrem').text('" + hour2 + ":" + newMinnutes2 + ":" + newSec2 + "');\r\n";
                if (tegi_kach["ТехВода1.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_p').css('background', 'gray');\r\n";
                else js += "$('#t1_p').css('background', 'white');\r\n";
                if (tegi_kach["ТехВода1.СКМ-2.Текущие параметры.Объемный расход канал 1"] != "good") js += "$('#t1_q').css('background', 'gray');\r\n";
                else js += "$('#t1_q').css('background', 'white');\r\n";
                if (tegi_kach["ТехВода1.СКМ-2.Текущие параметры.Объем канал 1"] != "good") js += "$('#t1_v').css('background', 'gray');\r\n";
                else js += "$('#t1_v').css('background', 'white');\r\n";
                if (tegi_kach["ТехВода1.СКМ-2.Group1.Tag1"] != "good") js += "$('#t1_vrem').css('background', 'gray');\r\n";
                else js += "$('#t1_vrem').css('background', 'white');\r\n";
                if (tegi_kach["ТехВода2.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_p').css('background', 'gray');\r\n";
                else js += "$('#t2_p').css('background', 'white');\r\n";
                if (tegi_kach["ТехВода2.СКМ-2.Текущие параметры.Объемный расход канал 1"] != "good") js += "$('#t2_q').css('background', 'gray');\r\n";
                else js += "$('#t2_q').css('background', 'white');\r\n";
                if (tegi_kach["ТехВода2.СКМ-2.Текущие параметры.Объем канал 1"] != "good") js += "$('#t2_v').css('background', 'gray');\r\n";
                else js += "$('#t2_v').css('background', 'white');\r\n";
                if (tegi_kach["ТехВода2.СКМ-2.Group1.Tag1"] != "good") js += "$('#t2_vrem').css('background', 'gray');\r\n";
                else js += "$('#t2_vrem').css('background', 'white');\r\n";
                js += "$('#tp2_p_T').text('" + tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/T"].ToString("0.##") + "');\r\n";
                js += "$('#tp2_p_P').text('" + ((tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/P"]-100)/1000).ToString("0.##") + "');\r\n";
                js += "$('#tp2_p_G').text('" + (tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#tp2_o_T').text('" + tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/T"].ToString("0.##") + "');\r\n";
                js += "$('#tp2_o_P').text('" + ((tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/P"]-100)/1000).ToString("0.##") + "');\r\n";
                js += "$('#tp2_o_G').text('" + (tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000).ToString("0.##") + "');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc1/T"] != "good") js += "$('#tp2_p_T').css('background', 'gray');\r\n";
                else js += "$('#tp2_p_T').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc1/P"] != "good") js += "$('#tp2_p_P').css('background', 'gray');\r\n";
                else js += "$('#tp2_p_P').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc1/q"] != "good") js += "$('#tp2_p_G').css('background', 'gray');\r\n";
                else js += "$('#tp2_p_G').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc2/T"] != "good") js += "$('#tp2_o_T').css('background', 'gray');\r\n";
                else js += "$('#tp2_o_T').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc2/P"] != "good") js += "$('#tp2_o_P').css('background', 'gray');\r\n";
                else js += "$('#tp2_o_P').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc2/q"] != "good") js += "$('#tp2_o_G').css('background', 'gray');\r\n";
                else js += "$('#tp2_o_G').css('background', 'white');\r\n";
                js += "$('#sh_p_T').text('" + tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/T"].ToString("0.##") + "');\r\n";
                js += "$('#sh_p_P').text('" + ((tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/P"]-100)/1000).ToString("0.##") + "');\r\n";
                js += "$('#sh_p_G').text('" + (tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#sh_o_T').text('" + tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/T"].ToString("0.##") + "');\r\n";
                js += "$('#sh_o_P').text('" + ((tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/P"]-100)/1000).ToString("0.##") + "');\r\n";
                js += "$('#sh_o_G').text('" + (tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000).ToString("0.##") + "');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc1/T"] != "good") js += "$('#sh_p_T').css('background', 'gray');\r\n";
                else js += "$('#sh_p_T').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc1/P"] != "good") js += "$('#sh_p_P').css('background', 'gray');\r\n";
                else js += "$('#sh_p_P').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc1/q"] != "good") js += "$('#sh_p_G').css('background', 'gray');\r\n";
                else js += "$('#sh_p_G').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc2/T"] != "good") js += "$('#sh_o_T').css('background', 'gray');\r\n";
                else js += "$('#sh_o_T').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc2/P"] != "good") js += "$('#sh_o_P').css('background', 'gray');\r\n";
                else js += "$('#sh_o_P').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc2/q"] != "good") js += "$('#sh_o_G').css('background', 'gray');\r\n";
                else js += "$('#sh_o_G').css('background', 'white');\r\n";
                js += "$('#tp2_dG').text('" + Math.Abs( tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/q"]/1000 - tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#sh_dG').text('" + Math.Abs(tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/q"]/1000 - tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#tp2_W').text('" + (tegi["tp2_MAZ/Counters/Pipeline/Pc1/W"] / 1000000 - tegi["tp2_MAZ/Counters/Pipeline/Pc2/W"] / 1000000).ToString("0.##") + "');\r\n";
                js += "$('#sh_W').text('" + (tegi["SH_MAZ/Counters/Pipeline/Pc1/W"] / 1000000 - tegi["SH_MAZ/Counters/Pipeline/Pc2/W"] / 1000000).ToString("0.##") + "');\r\n";

                js += "$('#tp2_qW').text(($('#tp2_W').html()*0.239).toFixed(1));\r\n";
                js += "$('#sh_qW').text(($('#sh_W').html()*0.239).toFixed(1));\r\n";

                js += "$('#t1_1stroka_1stolbec').text('" + tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/T"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_1stolbec').text('" + ((tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/P"]-100)/1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_1stolbec').text('" + (tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_1stolbec').text('" + tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/T"].ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_1stolbec').text('" + ((tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/P"]-100)/1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_1stolbec').text('" + (tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000).ToString("0.##") + "');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc1/T"] != "good") js += "$('#t1_1stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc1/P"] != "good") js += "$('#t1_3stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc1/q"] != "good") js += "$('#t1_5stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc2/T"] != "good") js += "$('#t1_2stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc2/P"] != "good") js += "$('#t1_4stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["tp2_MAZ/CurrentValues/Pipeline/Pc2/q"] != "good") js += "$('#t1_6stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_1stolbec').css('background', 'white');\r\n";
                js += "$('#t1_7stroka_1stolbec').text('" + Math.Abs(tegi["tp2_MAZ/CurrentValues/Pipeline/Pc1/q"] / 1000 - tegi["tp2_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_1stroka_2stolbec').text('" + tegi["Рамный1.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_2stolbec').text('" + tegi["Рамный1.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_2stolbec').text('" + ((Single)tegi["Рамный1.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_2stolbec').text('" + ((Single)tegi["Рамный1.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_2stolbec').text('" + tegi["Рамный1.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_2stolbec').text('" + tegi["Рамный1.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_2stolbec').text('" + Math.Abs(tegi["Рамный1.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["Рамный1.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["Рамный1.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный1.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный1.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный1.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный1.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный1.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_2stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_3stolbec').text('" + tegi["Рамный2.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_3stolbec').text('" + tegi["Рамный2.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_3stolbec').text('" + ((Single)tegi["Рамный2.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_3stolbec').text('" + ((Single)tegi["Рамный2.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_3stolbec').text('" + tegi["Рамный2.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_3stolbec').text('" + tegi["Рамный2.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_3stolbec').text('" + Math.Abs(tegi["Рамный2.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["Рамный2.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["Рамный2.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный2.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный2.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный2.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный2.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Рамный2.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_3stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_4stolbec').text('" + tegi["Нормали.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_4stolbec').text('" + tegi["Нормали.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_4stolbec').text('" + ((Single)tegi["Нормали.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_4stolbec').text('" + ((Single)tegi["Нормали.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_4stolbec').text('" + tegi["Нормали.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_4stolbec').text('" + tegi["Нормали.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_4stolbec').text('" + Math.Abs(tegi["Нормали.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["Нормали.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["Нормали.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Нормали.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Нормали.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Нормали.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Нормали.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Нормали.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_4stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_5stolbec').text('" + tegi["ЦПО.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_5stolbec').text('" + tegi["ЦПО.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_5stolbec').text('" + ((Single)tegi["ЦПО.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_5stolbec').text('" + ((Single)tegi["ЦПО.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_5stolbec').text('" + tegi["ЦПО.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_5stolbec').text('" + tegi["ЦПО.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_5stolbec').text('" + Math.Abs(tegi["ЦПО.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["ЦПО.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["ЦПО.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦПО.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦПО.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦПО.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦПО.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦПО.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_5stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_6stolbec').text('" + tegi["МСЦ-2.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_6stolbec').text('" + tegi["МСЦ-2.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_6stolbec').text('" + ((Single)tegi["МСЦ-2.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_6stolbec').text('" + ((Single)tegi["МСЦ-2.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_6stolbec').text('" + tegi["МСЦ-2.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_6stolbec').text('" + tegi["МСЦ-2.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_6stolbec').text('" + Math.Abs(tegi["МСЦ-2.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["МСЦ-2.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["МСЦ-2.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["МСЦ-2.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["МСЦ-2.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["МСЦ-2.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["МСЦ-2.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["МСЦ-2.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_6stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_7stolbec').text('" + tegi["Агрегатный.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_7stolbec').text('" + tegi["Агрегатный.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_7stolbec').text('" + ((Single)tegi["Агрегатный.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_7stolbec').text('" + ((Single)tegi["Агрегатный.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_7stolbec').text('" + tegi["Агрегатный.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_7stolbec').text('" + tegi["Агрегатный.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_7stolbec').text('" + Math.Abs(tegi["Агрегатный.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["Агрегатный.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["Агрегатный.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Агрегатный.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Агрегатный.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Агрегатный.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Агрегатный.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["Агрегатный.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_7stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_8stolbec').text('" + tegi["КЦ.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_8stolbec').text('" + tegi["КЦ.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_8stolbec').text('" + ((Single)tegi["КЦ.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_8stolbec').text('" + ((Single)tegi["КЦ.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_8stolbec').text('" + tegi["КЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_8stolbec').text('" + tegi["КЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_8stolbec').text('" + Math.Abs(tegi["КЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["КЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["КЦ.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЦ.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЦ.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЦ.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_8stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_9stolbec').text('" + tegi["MSK1 TP2.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_9stolbec').text('" + tegi["MSK1 TP2.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_9stolbec').text('" + ((Single)tegi["MSK1 TP2.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_9stolbec').text('" + ((Single)tegi["MSK1 TP2.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_9stolbec').text('" + tegi["MSK1 TP2.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_9stolbec').text('" + tegi["MSK1 TP2.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_9stolbec').text('" + Math.Abs(tegi["MSK1 TP2.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["MSK1 TP2.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["MSK1 TP2.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 TP2.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 TP2.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 TP2.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 TP2.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 TP2.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_9stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_10stolbec').text('" + tegi["ЦСиСА.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_10stolbec').text('" + tegi["ЦСиСА.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_10stolbec').text('" + ((Single)tegi["ЦСиСА.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_10stolbec').text('" + ((Single)tegi["ЦСиСА.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_10stolbec').text('" + tegi["ЦСиСА.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_10stolbec').text('" + tegi["ЦСиСА.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_10stolbec').text('" + Math.Abs(tegi["ЦСиСА.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["ЦСиСА.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["ЦСиСА.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦСиСА.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦСиСА.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦСиСА.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦСиСА.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦСиСА.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_10stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_11stolbec').text('" + tegi["SLC2 1.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_11stolbec').text('" + tegi["SLC2 1.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_11stolbec').text('" + ((Single)tegi["SLC2 1.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_11stolbec').text('" + ((Single)tegi["SLC2 1.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_11stolbec').text('" + tegi["SLC2 1.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_11stolbec').text('" + tegi["SLC2 1.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_11stolbec').text('" + Math.Abs(tegi["SLC2 1.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["SLC2 1.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["SLC2 1.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 1.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 1.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 1.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 1.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 1.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_11stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_12stolbec').text('" + tegi["SLC2 2.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_12stolbec').text('" + tegi["SLC2 2.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_12stolbec').text('" + ((Single)tegi["SLC2 2.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_12stolbec').text('" + ((Single)tegi["SLC2 2.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_12stolbec').text('" + tegi["SLC2 2.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_12stolbec').text('" + tegi["SLC2 2.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_12stolbec').text('" + Math.Abs(tegi["SLC2 2.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["SLC2 2.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["SLC2 2.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_12stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_12stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 2.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_12stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_12stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 2.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_12stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_12stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 2.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_12stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_12stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 2.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_12stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_12stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SLC2 2.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_12stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_12stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_13stolbec').text('" + tegi["CPl.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_13stolbec').text('" + tegi["CPl.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_13stolbec').text('" + ((Single)tegi["CPl.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_13stolbec').text('" + ((Single)tegi["CPl.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_13stolbec').text('" + tegi["CPl.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_13stolbec').text('" + tegi["CPl.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_13stolbec').text('" + Math.Abs(tegi["CPl.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["CPl.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["CPl.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_13stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_13stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CPl.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_13stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_13stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CPl.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_13stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_13stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CPl.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_13stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_13stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CPl.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_13stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_13stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CPl.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_13stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_13stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_14stolbec').text('" + tegi["COM.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_14stolbec').text('" + tegi["COM.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_14stolbec').text('" + ((Single)tegi["COM.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_14stolbec').text('" + ((Single)tegi["COM.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_14stolbec').text('" + tegi["COM.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_14stolbec').text('" + tegi["COM.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_14stolbec').text('" + Math.Abs(tegi["COM.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["COM.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["COM.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_14stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_14stolbec').css('background', 'white');\r\n";
                if (tegi_kach["COM.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_14stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_14stolbec').css('background', 'white');\r\n";
                if (tegi_kach["COM.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_14stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_14stolbec').css('background', 'white');\r\n";
                if (tegi_kach["COM.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_14stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_14stolbec').css('background', 'white');\r\n";
                if (tegi_kach["COM.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_14stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_14stolbec').css('background', 'white');\r\n";
                if (tegi_kach["COM.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_14stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_14stolbec').css('background', 'white');\r\n";
                js += "$('#t1_1stroka_15stolbec').text('" + tegi["CSiOK.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_2stroka_15stolbec').text('" + tegi["CSiOK.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_3stroka_15stolbec').text('" + ((Single)tegi["CSiOK.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_4stroka_15stolbec').text('" + ((Single)tegi["CSiOK.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t1_5stroka_15stolbec').text('" + tegi["CSiOK.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t1_6stroka_15stolbec').text('" + tegi["CSiOK.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t1_7stroka_15stolbec').text('" + Math.Abs(tegi["CSiOK.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["CSiOK.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["CSiOK.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t1_1stroka_15stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_1stroka_15stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiOK.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t1_2stroka_15stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_2stroka_15stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiOK.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t1_3stroka_15stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_3stroka_15stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiOK.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t1_4stroka_15stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_4stroka_15stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiOK.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t1_5stroka_15stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_5stroka_15stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiOK.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t1_6stroka_15stolbec').css('background', 'gray');\r\n";
                else js += "$('#t1_6stroka_15stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_1stolbec').text('" + tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/T"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_1stolbec').text('" + ((tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/P"]-100)/1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_1stolbec').text('" + (tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_1stolbec').text('" + tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/T"].ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_1stolbec').text('" + ((tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/P"]-100)/1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_1stolbec').text('" + (tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_1stolbec').text('" + Math.Abs(tegi["SH_MAZ/CurrentValues/Pipeline/Pc1/q"] / 1000 - tegi["SH_MAZ/CurrentValues/Pipeline/Pc2/q"]/1000).ToString("0.##") + "');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc1/T"] != "good") js += "$('#t2_1stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc1/P"] != "good") js += "$('#t2_3stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc1/q"] != "good") js += "$('#t2_5stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc2/T"] != "good") js += "$('#t2_2stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc2/P"] != "good") js += "$('#t2_4stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_1stolbec').css('background', 'white');\r\n";
                if (tegi_kach["SH_MAZ/CurrentValues/Pipeline/Pc2/q"] != "good") js += "$('#t2_6stroka_1stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_1stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_2stolbec').text('" + tegi["MSK1 SH.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_2stolbec').text('" + tegi["MSK1 SH.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_2stolbec').text('" + ((Single)tegi["MSK1 SH.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_2stolbec').text('" + ((Single)tegi["MSK1 SH.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_2stolbec').text('" + tegi["MSK1 SH.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_2stolbec').text('" + tegi["MSK1 SH.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_2stolbec').text('" + Math.Abs(tegi["MSK1 SH.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["MSK1 SH.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["MSK1 SH.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 SH.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 SH.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 SH.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 SH.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_2stolbec').css('background', 'white');\r\n";
                if (tegi_kach["MSK1 SH.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_2stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_2stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_3stolbec').text('" + tegi["КЗЦ.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_3stolbec').text('" + tegi["КЗЦ.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_3stolbec').text('" + ((Single)tegi["КЗЦ.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_3stolbec').text('" + ((Single)tegi["КЗЦ.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_3stolbec').text('" + tegi["КЗЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_3stolbec').text('" + tegi["КЗЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_3stolbec').text('" + Math.Abs(tegi["КЗЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["КЗЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["КЗЦ.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЗЦ.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЗЦ.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЗЦ.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЗЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_3stolbec').css('background', 'white');\r\n";
                if (tegi_kach["КЗЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_3stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_3stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_4stolbec').text('" + tegi["PrC.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_4stolbec').text('" + tegi["PrC.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_4stolbec').text('" + ((Single)tegi["PrC.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_4stolbec').text('" + ((Single)tegi["PrC.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_4stolbec').text('" + tegi["PrC.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_4stolbec').text('" + tegi["PrC.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_4stolbec').text('" + Math.Abs(tegi["PrC.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["PrC.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["PrC.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["PrC.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["PrC.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["PrC.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["PrC.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_4stolbec').css('background', 'white');\r\n";
                if (tegi_kach["PrC.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_4stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_4stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_5stolbec').text('" + tegi["ЦМШ.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_5stolbec').text('" + tegi["ЦМШ.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_5stolbec').text('" + ((Single)tegi["ЦМШ.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_5stolbec').text('" + ((Single)tegi["ЦМШ.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_5stolbec').text('" + tegi["ЦМШ.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_5stolbec').text('" + tegi["ЦМШ.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_5stolbec').text('" + Math.Abs(tegi["ЦМШ.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["ЦМШ.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["ЦМШ.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦМШ.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦМШ.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦМШ.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦМШ.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_5stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЦМШ.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_5stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_5stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_6stolbec').text('" + tegi["ISHZ.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_6stolbec').text('" + tegi["ISHZ.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_6stolbec').text('" + ((Single)tegi["ISHZ.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_6stolbec').text('" + ((Single)tegi["ISHZ.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_6stolbec').text('" + tegi["ISHZ.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_6stolbec').text('" + tegi["ISHZ.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_6stolbec').text('" + Math.Abs(tegi["ISHZ.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["ISHZ.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["ISHZ.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ISHZ.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ISHZ.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ISHZ.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ISHZ.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_6stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ISHZ.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_6stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_6stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_7stolbec').text('" + tegi["ЭЦ1.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_7stolbec').text('" + tegi["ЭЦ1.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_7stolbec').text('" + ((Single)tegi["ЭЦ1.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_7stolbec').text('" + ((Single)tegi["ЭЦ1.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_7stolbec').text('" + tegi["ЭЦ1.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_7stolbec').text('" + tegi["ЭЦ1.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_7stolbec').text('" + Math.Abs(tegi["ЭЦ1.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["ЭЦ1.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["ЭЦ1.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЭЦ1.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЭЦ1.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЭЦ1.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЭЦ1.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_7stolbec').css('background', 'white');\r\n";
                if (tegi_kach["ЭЦ1.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_7stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_7stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_8stolbec').text('" + tegi["CSMA.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_8stolbec').text('" + tegi["CSMA.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_8stolbec').text('" + ((Single)tegi["CSMA.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_8stolbec').text('" + ((Single)tegi["CSMA.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_8stolbec').text('" + tegi["CSMA.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_8stolbec').text('" + tegi["CSMA.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_8stolbec').text('" + Math.Abs(tegi["CSMA.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["CSMA.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["CSMA.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSMA.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSMA.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSMA.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSMA.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_8stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSMA.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_8stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_8stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_9stolbec').text('" + tegi["АТЦ.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_9stolbec').text('" + tegi["АТЦ.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_9stolbec').text('" + ((Single)tegi["АТЦ.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_9stolbec').text('" + ((Single)tegi["АТЦ.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_9stolbec').text('" + tegi["АТЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_9stolbec').text('" + tegi["АТЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_9stolbec').text('" + Math.Abs(tegi["АТЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["АТЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["АТЦ.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["АТЦ.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["АТЦ.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["АТЦ.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["АТЦ.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_9stolbec').css('background', 'white');\r\n";
                if (tegi_kach["АТЦ.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_9stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_9stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_10stolbec').text('" + tegi["CSiSA 24c.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_10stolbec').text('" + tegi["CSiSA 24c.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_10stolbec').text('" + ((Single)tegi["CSiSA 24c.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_10stolbec').text('" + ((Single)tegi["CSiSA 24c.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_10stolbec').text('" + tegi["CSiSA 24c.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_10stolbec').text('" + tegi["CSiSA 24c.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_10stolbec').text('" + Math.Abs(tegi["CSiSA 24c.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["CSiSA 24c.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["CSiSA 24c.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiSA 24c.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiSA 24c.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiSA 24c.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiSA 24c.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_10stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CSiSA 24c.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_10stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_10stolbec').css('background', 'white');\r\n";
                js += "$('#t2_1stroka_11stolbec').text('" + tegi["CAA.СКМ-2.Текущие параметры.Температура канала 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_2stroka_11stolbec').text('" + tegi["CAA.СКМ-2.Текущие параметры.Температура канала 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_3stroka_11stolbec').text('" + ((Single)tegi["CAA.СКМ-2.Текущие параметры.Давление канал 1"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_4stroka_11stolbec').text('" + ((Single)tegi["CAA.СКМ-2.Текущие параметры.Давление канал 2"] / 1000).ToString("0.##") + "');\r\n";
                js += "$('#t2_5stroka_11stolbec').text('" + tegi["CAA.СКМ-2.Текущие параметры.Массовый расход канал 1"].ToString("0.##") + "');\r\n";
                js += "$('#t2_6stroka_11stolbec').text('" + tegi["CAA.СКМ-2.Текущие параметры.Массовый расход канал 2"].ToString("0.##") + "');\r\n";
                js += "$('#t2_7stroka_11stolbec').text('" + Math.Abs(tegi["CAA.СКМ-2.Текущие параметры.Массовый расход канал 1"] - tegi["CAA.СКМ-2.Текущие параметры.Массовый расход канал 2"]).ToString("0.##") + "');\r\n";
                if (tegi_kach["CAA.СКМ-2.Текущие параметры.Температура канала 1"] != "good") js += "$('#t2_1stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_1stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CAA.СКМ-2.Текущие параметры.Температура канала 2"] != "good") js += "$('#t2_2stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_2stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CAA.СКМ-2.Текущие параметры.Давление канал 1"] != "good") js += "$('#t2_3stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_3stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CAA.СКМ-2.Текущие параметры.Давление канал 2"] != "good") js += "$('#t2_4stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_4stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CAA.СКМ-2.Текущие параметры.Массовый расход канал 1"] != "good") js += "$('#t2_5stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_5stroka_11stolbec').css('background', 'white');\r\n";
                if (tegi_kach["CAA.СКМ-2.Текущие параметры.Массовый расход канал 2"] != "good") js += "$('#t2_6stroka_11stolbec').css('background', 'gray');\r\n";
                else js += "$('#t2_6stroka_11stolbec').css('background', 'white');\r\n";
                js += "});";
                string _file = @"c:\node_js\javascript\tegi_all.js";

                using (var fs = new FileStream(_file, FileMode.Truncate, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.Write(js); }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }
        void обновить_таблицу_данные_10_min()
        {
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_ТехВода1", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _M1 = tegi["ТехВода1.СКМ-2.Текущие параметры.Массовый расход канал 1"];
                    Single _p1 = tegi["ТехВода1.СКМ-2.Текущие параметры.Давление канал 1"];       
                    Single Δm =  tegi["ТехВода1.СКМ-2.Текущие параметры.Объем канал 1"];         
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = Δm;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["p1"] = _p1;
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }

                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_ТехВода2", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _M1 = tegi["ТехВода2.СКМ-2.Текущие параметры.Массовый расход канал 1"];
                    Single _p1 = tegi["ТехВода2.СКМ-2.Текущие параметры.Давление канал 1"];
                    Single Δm = tegi["ТехВода2.СКМ-2.Текущие параметры.Объем канал 1"];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = Δm;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["p1"] = _p1;
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_АртВода", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _M1 = tegi["АртВода.СКМ-2.Текущие параметры.Массовый расход канал 1"];
                    Single _p1 = tegi["АртВода.СКМ-2.Текущие параметры.Давление канал 1"];
                    Single Δm =  tegi["АртВода.СКМ-2.Текущие параметры.Объем канал 1"];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = Δm;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["p1"] = Math.Round(_p1, 3);
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }

                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_ХимОчищВода", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    Single _M1 = tegi["ХимОчищВода.СКМ-2.Текущие параметры.Массовый расход канал 1"];
                    Single _p1 = tegi["ХимОчищВода.СКМ-2.Текущие параметры.Давление канал 1"];
                    Single Δm =  tegi["ХимОчищВода.СКМ-2.Текущие параметры.Объем канал 1"];
                    Single T1 =  tegi["ХимОчищВода.СКМ-2.Текущие параметры.Температура канала 1"];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                        dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                        dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                        dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                        dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                        dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                        dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                        dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                    }
                    dt.Rows[999]["Data"] = data + timeSaved;
                    dt.Rows[999]["Δm"] = Δm;
                    dt.Rows[999]["M1"] = _M1;
                    dt.Rows[999]["p1"] = Math.Round(_p1, 3);
                    dt.Rows[999]["t1"] = T1;
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
                DataTable dt1 = null;
                DataTable dt2 = null;
                DataTable dt3 = null;
                //string sql = "SELECT * FROM OPC_data_АртВода";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT Data AS 'Время', Δm AS 'Объем', p1 AS 'Давление', M1 AS 'Расход' FROM OPC_data_АртВода", connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1 = ds.Tables[0];
                }
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_ХимОчищВода", connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt2 = ds.Tables[0];
                }
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM АртХим", connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем DatasetOPC_data_ХимОчищВода
                    adapter.Fill(ds);
                    dt3 = ds.Tables[0];
                    for (int i = 0; i < dt3.Rows.Count; i++)
                    {
                        dt3.Rows[i]["Время_измерения"] = dt1.Rows[i]["Время"];
                        dt3.Rows[i]["арт_P"] = Math.Round((Single)dt1.Rows[i]["Давление"] / 1000, 2);
                        dt3.Rows[i]["арт_Q"] = Math.Round((Single)dt1.Rows[i]["Расход"], 2);
                        dt3.Rows[i]["хим_P"] = Math.Round((Single)dt2.Rows[i]["p1"] / 1000, 2);
                        dt3.Rows[i]["хим_Q"] = Math.Round((Single)dt2.Rows[i]["M1"], 2);
                        dt3.Rows[i]["хим_T"] = Math.Round((Single)dt2.Rows[i]["t1"], 2);
                    }

                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                }
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_teplos_par", connection);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    DataTable dt = ds.Tables[0];
                    for (int z = 1; z < 1000; z++)
                    {
                        dt.Rows[z - 1]["time"] = dt.Rows[z]["time"];
                        dt.Rows[z - 1]["T"] = dt.Rows[z]["T"];
                        dt.Rows[z - 1]["P"] = dt.Rows[z]["P"];
                        dt.Rows[z - 1]["G"] = dt.Rows[z]["G"];
                    }
                    dt.Rows[999]["time"] = data + timeSaved;
                    dt.Rows[999]["T"] = tegi["СПСеть.SPT961_1M.т1.156(T)"];
                    dt.Rows[999]["P"] = tegi["СПСеть.SPT961_1M.т1.154(P)"];
                    dt.Rows[999]["G"] = tegi["СПСеть.SPT961_1M.т1.157(G)"];
                    SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                    adapter.Update(ds);
                    ds.Clear();
                    // перезагружаем данные
                    adapter.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }
        }

        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
           //e.Cancel = true;
        }
        private void scripts2()
        {
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                DataTable dt1 = null;
                DataTable dt2 = null;
                DataTable dt3 = null;
                //string sql = "SELECT * FROM OPC_data_АртВода";
                string sql = "SELECT Data AS 'Время', Δm AS 'Объем', p1 AS 'Давление', M1 AS 'Расход' FROM OPC_data_АртВода";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_data_ХимОчищВода";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt2 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_data_teplos_par";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt3 = ds.Tables[0];
                }
                string js = "$(document).ready(function() {\r\n";
                for (int i = 0; i < 1000; i++)
                {
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1.Rows[i]["Время"].ToString() + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_2stolbec').html('" + Math.Round((Single)dt1.Rows[i]["Объем"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_3stolbec').html('" + Math.Round((Single)dt1.Rows[i]["Давление"] / 1000, 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_4stolbec').html('" + Math.Round((Single)dt1.Rows[i]["Расход"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_5stolbec').html('" + Math.Round((Single)dt2.Rows[i]["Δm"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_6stolbec').html('" + Math.Round((Single)dt2.Rows[i]["p1"] / 1000, 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_7stolbec').html('" + Math.Round((Single)dt2.Rows[i]["M1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_8stolbec').html('" + Math.Round((Single)dt2.Rows[i]["t1"], 2) + "');\r\n";

                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_9stolbec').html('" +  Math.Round((Single)dt3.Rows[i]["T"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_10stolbec').html('" + Math.Round((Single)dt3.Rows[i]["P"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_11stolbec').html('" + Math.Round((Single)dt3.Rows[i]["G"], 2) + "');\r\n";
                }
                //js += "const element = document.getElementById('AXP_1000stroka_8stolbec');element.scrollIntoView({ block: 'end' });";
                js += "});";
                string _file = @"c:\node_js\javascript\par_art_him\table_art_him.js";
                using (var fs = new FileStream(_file, FileMode.Truncate, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.Write(js); }

            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }

        }
        private void scripts_3()
        {
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                DataTable dt1 = null;

                //string sql = "SELECT * FROM OPC_data_АртВода";
                string sql = "SELECT * FROM OPC_chas_PAR";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1 = ds.Tables[0];
                }

                string js = "$(document).ready(function() {\r\n";
                for (int i = 0; i < 1000; i++)
                {
                    js += "$('#par_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1.Rows[i]["time"].ToString() + "');\r\n";
                    js += "$('#par_" + (i + 1).ToString() + "stroka_2stolbec').html('" + Math.Round((Single)dt1.Rows[i]["T"], 2) + "');\r\n";
                    js += "$('#par_" + (i + 1).ToString() + "stroka_3stolbec').html('" + Math.Round((Single)dt1.Rows[i]["P"], 2) + "');\r\n";
                    js += "$('#par_" + (i + 1).ToString() + "stroka_4stolbec').html('" + Math.Round((Single)dt1.Rows[i]["G"], 2) + "');\r\n";
                }
                //js += "const element = document.getElementById('AXP_1000stroka_8stolbec');element.scrollIntoView({ block: 'end' });";
                js += "});";
                string _file = @"c:\node_js\javascript\chas_par.js";
                using (var fs = new FileStream(_file, FileMode.Truncate, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs)) { sw.Write(js); }

            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }

        }
        private void scripts_den()
        {
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                DataTable dt1 = null;
                DataTable dt2 = null;
                DataTable dt3 = null;
                //string sql = "SELECT * FROM OPC_data_АртВода";
                string sql = "SELECT * FROM OPC_den_АртВода";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_den_ХимОчищВода";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt2 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_den_teplos_par";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt3 = ds.Tables[0];
                }
                string js = "$(document).ready(function() {\r\n";
                for (int i = 0; i < 1000; i++)
                {
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1.Rows[i]["Data"].ToString() + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_3stolbec').html('" + Math.Round((Single)dt1.Rows[i]["p1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_4stolbec').html('" + Math.Round((Single)dt1.Rows[i]["M1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_6stolbec').html('" + Math.Round((Single)dt2.Rows[i]["p1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_7stolbec').html('" + Math.Round((Single)dt2.Rows[i]["M1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_8stolbec').html('" + Math.Round((Single)dt2.Rows[i]["t1"], 2) + "');\r\n";

                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_9stolbec').html('" +  Math.Round((Single)dt3.Rows[i]["T"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_10stolbec').html('" + Math.Round((Single)dt3.Rows[i]["P"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_11stolbec').html('" + Math.Round((Single)dt3.Rows[i]["G"], 2) + "');\r\n";
                }
                //js += "const element = document.getElementById('AXP_1000stroka_8stolbec');element.scrollIntoView({ block: 'end' });";
                js += "});";
                string _file = @"c:\node_js\javascript\par_art_him\den_table_art_him.js";


                if (!File.Exists(_file))
                {
                    using (File.Create(_file)) { } // Создать файл, если он не существует
                }

                using (var fs = new FileStream(_file, FileMode.Truncate, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs))
                {
                    sw.Write(js); // Записываем данные в файл
                }

            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }

        }
        private void scripts3()
        {
            try
            {
                string data = DateTime.Now.ToString("yyyy_MM_dd");
                DataTable dt1 = null;
                DataTable dt2 = null;
                DataTable dt3 = null;
                //string sql = "SELECT * FROM OPC_data_АртВода";
                string sql = "SELECT * FROM OPC_chas_АртВода";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt1 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_chas_ХимОчищВода";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt2 = ds.Tables[0];
                }
                sql = "SELECT * FROM OPC_chas_teplos_par";
                using (SqlConnection connection = new SqlConnection(server_name))
                {
                    // Создаем объект DataAdapter
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                    // Создаем объект Dataset
                    DataSet ds = new DataSet();
                    // Заполняем Dataset
                    adapter.Fill(ds);
                    dt3 = ds.Tables[0];
                }
                string js = "$(document).ready(function() {\r\n";
                for (int i = 0; i < 1000; i++)
                {
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_1stolbec').html('" + dt1.Rows[i]["Data"].ToString() + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_3stolbec').html('" + Math.Round((Single)dt1.Rows[i]["p1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_4stolbec').html('" + Math.Round((Single)dt1.Rows[i]["M1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_6stolbec').html('" + Math.Round((Single)dt2.Rows[i]["p1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_7stolbec').html('" + Math.Round((Single)dt2.Rows[i]["M1"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_8stolbec').html('" + Math.Round((Single)dt2.Rows[i]["t1"], 2) + "');\r\n";

                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_9stolbec').html('" + Math.Round((Single)dt3.Rows[i]["T"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_10stolbec').html('" + Math.Round((Single)dt3.Rows[i]["P"], 2) + "');\r\n";
                    js += "$('#AXP_" + (i + 1).ToString() + "stroka_11stolbec').html('" + Math.Round((Single)dt3.Rows[i]["G"], 2) + "');\r\n";
                }
                //js += "const element = document.getElementById('AXP_1000stroka_8stolbec');element.scrollIntoView({ block: 'end' });";
                js += "});";
                string _file = @"c:\node_js\javascript\par_art_him\chas_table_art_him.js";


                if (!File.Exists(_file))
                {
                    using (File.Create(_file)) { } // Создать файл, если он не существует
                }

                using (var fs = new FileStream(_file, FileMode.Truncate, FileAccess.Write, FileShare.ReadWrite))
                using (var sw = new StreamWriter(fs))
                {
                    sw.Write(js); // Записываем данные в файл
                }

            }
            catch (Exception ex)
            {
                error(ex.Message + ex.StackTrace);
            }

        }
        private void script_teh_voda()
        {                
            string data = DateTime.Now.ToString("yyyy_MM_dd");
            string timeSaved = DateTime.Now.ToString(" H:mm:ss ");
            using (SqlConnection connection = new SqlConnection(server_name))
            {

                SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_ТехВода1", connection);
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable dt = ds.Tables[0];
                Single _M1 = tegi["ТехВода1.СКМ-2.Текущие параметры.Массовый расход канал 1"];
                Single _p1 = tegi["ТехВода1.СКМ-2.Текущие параметры.Давление канал 1"];
                Single Δm = tegi["ТехВода1.СКМ-2.Текущие параметры.Объем канал 1"];//
                for (int z = 1; z < 1000; z++)
                {
                    dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                    dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                    dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                    dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                    dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                    dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                    dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                    dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                }
                dt.Rows[999]["Data"] = data + timeSaved;
                dt.Rows[999]["Δm"] = Δm;
                dt.Rows[999]["M1"] = _M1;
                dt.Rows[999]["p1"] = _p1;
                SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                adapter.Update(ds);
                ds.Clear();
                // перезагружаем данные
                adapter.Fill(ds);
            }
            using (SqlConnection connection = new SqlConnection(server_name))
            {
                SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM OPC_data_ТехВода2", connection);
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                DataTable dt = ds.Tables[0];
                Single _M1 = tegi["ТехВода2.СКМ-2.Текущие параметры.Массовый расход канал 1"];
                Single _p1 = tegi["ТехВода2.СКМ-2.Текущие параметры.Давление канал 1"];
                Single Δm = tegi["ТехВода2.СКМ-2.Текущие параметры.Объем канал 1"];
                for (int z = 1; z < 1000; z++)
                {
                    dt.Rows[z - 1]["Data"] = dt.Rows[z]["Data"];
                    dt.Rows[z - 1]["Δm"] = dt.Rows[z]["Δm"];
                    dt.Rows[z - 1]["M1"] = dt.Rows[z]["M1"];
                    dt.Rows[z - 1]["M2"] = dt.Rows[z]["M2"];
                    dt.Rows[z - 1]["t1"] = dt.Rows[z]["t1"];
                    dt.Rows[z - 1]["t2"] = dt.Rows[z]["t2"];
                    dt.Rows[z - 1]["p1"] = dt.Rows[z]["p1"];
                    dt.Rows[z - 1]["p2"] = dt.Rows[z]["p2"];
                }
                dt.Rows[999]["Data"] = data + timeSaved;
                dt.Rows[999]["Δm"] = Δm;
                dt.Rows[999]["M1"] = _M1;
                dt.Rows[999]["p1"] = _p1;
                SqlCommandBuilder commandBuilder = new SqlCommandBuilder(adapter);
                adapter.Update(ds);
                ds.Clear();
                // перезагружаем данные
                adapter.Fill(ds);
            }
        }


        private void checkBox1_CheckedChanged(object sender, EventArgs e)
        {
            if (checkBox1.Checked == true) button1.Visible = true;
            else button1.Visible = false;
        }
        private void _reset()
        {
            // Получаем все процессы с именем "node"
            var processes = Process.GetProcessesByName("node");
            if (processes.Length > 0) // Проверяем, существует ли процесс
            {
                // Завершите процесс сервера
                foreach (var process in processes)
                {
                    process.Kill();
                    process.WaitForExit(); // Ждем завершения процесса
                }
            }
            Thread.Sleep(1000);
            processes = Process.GetProcessesByName("DAS");
            if (processes.Length > 0) // Проверяем, существует ли процесс
            {
                // Завершите процесс сервера
                foreach (var process in processes)
                {
                    process.Kill();
                }
            }
            Thread.Sleep(1000);
            // Получаем путь к текущему исполняемому файлу
            string exePath = Application.ExecutablePath;

            // Запускаем новый процесс
            Process.Start(exePath);

            // Завершаем текущий процесс
            Application.Exit();

        }
        private void button1_Click(object sender, EventArgs e)
        {
            _reset();
        }

        private void label1_Click(object sender, EventArgs e)
        {
            try { обновить_узлы("den"); } catch (Exception ex) { error(ex.Message + ex.StackTrace); }
        }
    }
    class dann_teg
    {
        public string имя_тега { get; set; }
        public float значение { get; set; }
        public int итераций { get; set; }
        public float суточное_значение { get; set; } // Для хранения суммы за сутки
        public int суточные_итерации { get; set; } // Для хранения количества итераций за сутки

        public dann_teg(string name, float value, int iter)
        {
            имя_тега = name;
            значение = value;
            итераций = iter;
            суточное_значение = 0f;
            суточные_итерации = 0;
        }
    }
    // Структура для хранения данных тега
    public struct TagData
    {
        public float Sum { get; set; }
        public int Count { get; set; }
        public string Quality { get; set; }

        public float Average => Count > 0 ? Sum / Count : 0;
    }

}
