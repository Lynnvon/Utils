using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Collections;
using System.Windows.Forms;
using System.IO;
using System.ComponentModel;
using System.Net.Mime;
using System.Threading;
using System.Xml;
using Utils;

namespace Utils.Update
{
    public class AutoUpdate
    {
        #region 成员与字段属性

        private string _updaterUrl;
        private string _serverUrl;
        public ArrayList FileList = new ArrayList();
        private bool disposed = false;
        private IntPtr handle;
        private Component component = new Component();
        private BackgroundWorker worker;
        public string _oldVer;
        public string _newVer;
        public string updateContent;

        [System.Runtime.InteropServices.DllImport("Kernel32")]
        private extern static Boolean CloseHandle(IntPtr handle);


        public string UpdaterUrl
        {
            set { _updaterUrl = value; }
            get { return this._updaterUrl; }
        }

        #endregion

        /// <summary>
        /// AppUpdater构造函数
        /// </summary>
        public AutoUpdate()
        {
            //this.handle = handle;
            this.handle = IntPtr.Zero;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {

                    component.Dispose();
                }
                CloseHandle(handle);
                handle = IntPtr.Zero;
            }
            disposed = true;
        }

        ~AutoUpdate()
        {
            Dispose(false);
        }


        /// <summary>
        /// 检查更新文件
        /// </summary>
        /// <param name="serverXmlFile"></param>
        /// <param name="localXmlFile"></param>
        /// <param name="updateFileList"></param>
        /// <returns></returns>
        public int CheckForUpdate(string serverXmlFile, string localXmlFile, out Hashtable updateFileList)
        {

            updateFileList = new Hashtable();
            if (!File.Exists(localXmlFile) || !File.Exists(serverXmlFile))
            {
                return -1;
            }

            XmlFiles serverXmlFiles = new XmlFiles(serverXmlFile);
            XmlFiles localXmlFiles = new XmlFiles(localXmlFile);

            XmlNodeList newNodeList = serverXmlFiles.GetNodeList("AutoUpdater/Files");
            XmlNodeList oldNodeList = localXmlFiles.GetNodeList("AutoUpdater/Files"); //5+1+a+s+p+x

            int k = 0;
            for (int i = 0; i < newNodeList.Count; i++)
            {
                string[] fileList = new string[3];

                string newFileName = newNodeList.Item(i).Attributes["Name"].Value.Trim();
                string newVer = newNodeList.Item(i).Attributes["Ver"].Value.Trim();

                ArrayList oldFileAl = new ArrayList();
                for (int j = 0; j < oldNodeList.Count; j++)
                {
                    string oldFileName = oldNodeList.Item(j).Attributes["Name"].Value.Trim();
                    string oldVer = oldNodeList.Item(j).Attributes["Ver"].Value.Trim();

                    oldFileAl.Add(oldFileName);
                    oldFileAl.Add(oldVer);

                }
                int pos = oldFileAl.IndexOf(newFileName);
                if (pos == -1)
                {
                    fileList[0] = newFileName;
                    fileList[1] = newVer;
                    updateFileList.Add(k, fileList);
                    k++;
                }
                else if (pos > -1 && newVer.CompareTo(oldFileAl[pos + 1].ToString()) > 0)
                {
                    fileList[0] = newFileName;
                    fileList[1] = newVer;
                    updateFileList.Add(k, fileList);
                    k++;
                }

            }
            return k;
        }
        void SetProgress(int sleepTime, int progress, string content)
        {
            Thread.Sleep(sleepTime);
            worker.ReportProgress(progress, content);
        }
        /// <summary>
        /// 检查更新文件
        /// </summary>
        /// <param name="serverXmlFile"></param>
        /// <param name="localXmlFile"></param>
        /// <param name="updateFileList"></param>
        /// <returns></returns>
        public int CheckForUpdate(BackgroundWorker _worker)
        {
            this.worker = _worker;
            string localXmlFile = Application.StartupPath + "\\AutoUpdate.xml";
            if (!File.Exists(localXmlFile))
            {
                return -1;
            }

            XmlFiles updaterXmlFiles = new XmlFiles(localXmlFile);


            //string tempUpdatePath = AppDomain.CurrentDomain.BaseDirectory + "\\AutoUpdate\\" + "_" +
            //                        updaterXmlFiles.FindNode("//Application").Attributes["applicationId"].Value + "_" +
            //                        "y" + "_" + "x" + "_" + "m" + "_" + "\\";
            string tempUpdatePath = Application.StartupPath + "\\AutoUpdate\\" + "_" +
                                updaterXmlFiles.FindNode("//Application").Attributes["applicationId"].Value + "_" +
                                "y" + "_" + "x" + "_" + "m" + "_" + "\\";
            this.UpdaterUrl = updaterXmlFiles.GetNodeValue("//Url") + "/AutoUpdate.xml";
            this._serverUrl = updaterXmlFiles.GetNodeValue("//Url");
            this.DownAutoUpdateFile(tempUpdatePath);


            string serverXmlFile = tempUpdatePath + "\\AutoUpdate.xml";
            if (!File.Exists(serverXmlFile))
            {
                return -1;
            }

            XmlFiles serverXmlFiles = new XmlFiles(serverXmlFile);
            XmlFiles localXmlFiles = new XmlFiles(localXmlFile);

            _oldVer = localXmlFiles.GetNodeValue("AutoUpdater/Application/Version");
            _newVer = serverXmlFiles.GetNodeValue("AutoUpdater/Application/Version");
            updateContent = serverXmlFiles.GetNodeValue("AutoUpdater/UpdateContent");


            XmlNodeList newNodeList = serverXmlFiles.GetNodeList("AutoUpdater/Files");
            XmlNodeList oldNodeList = localXmlFiles.GetNodeList("AutoUpdater/Files");

            ArrayList oldFileAl = new ArrayList();
            for (int j = 0; j < oldNodeList.Count; j++)
            {
                string oldFileName = oldNodeList.Item(j).Attributes["Name"].Value.Trim();
                string oldVer = oldNodeList.Item(j).Attributes["Ver"].Value.Trim();

                oldFileAl.Add(oldFileName);
                oldFileAl.Add(oldVer);
            }

            int k = 0;
            for (int i = 0; i < newNodeList.Count; i++)
            {

                string newFileName = newNodeList.Item(i).Attributes["Name"].Value.Trim();
                string newVer = newNodeList.Item(i).Attributes["Ver"].Value.Trim();

                int pos = oldFileAl.IndexOf(newFileName);
                if (pos == -1)
                {
                    FileList.Add(newFileName);
                    k++;
                }
                else if (pos > -1 && newVer.CompareTo(oldFileAl[pos + 1].ToString()) > 0)
                {
                    FileList.Add(newFileName);
                    k++;
                }
            }

            return k;
        }

        /// <summary>
        /// 返回下载更新文件的临时目录
        /// </summary>
        /// <returns></returns>
        public void DownAutoUpdateFile(string downpath)
        {
            if (!System.IO.Directory.Exists(downpath))
                System.IO.Directory.CreateDirectory(downpath);
            string serverXmlFile = downpath + @"/AutoUpdate.xml";

            try
            {
                WebRequest req = WebRequest.Create(this.UpdaterUrl);
                using (WebResponse res = req.GetResponse())
                {
                    if (res.ContentLength > 0)
                    {
                        try
                        {
                            WebClient wClient = new WebClient();
                            wClient.DownloadFile(this.UpdaterUrl, serverXmlFile);
                        }
                        catch
                        {
                            return;
                        }
                    }
                }
            }
            catch
            {
                return;
            }
            //return tempPath;
        }

        public void DownLoadFile()
        {
            try
            {
                WebRequest req = WebRequest.Create(this.UpdaterUrl);
                using (WebResponse res = req.GetResponse())
                {
                    if (res.ContentLength > 0)
                    {
                        try
                        {
                            WebClient wClient = new WebClient();
                            foreach (var VARIABLE in FileList)
                            {
                                if (VARIABLE != null)
                                {
                                    try
                                    {
                                        wClient.DownloadFile(_serverUrl + VARIABLE, VARIABLE.ToString());
                                    }
                                    catch (WebException ee)
                                    {

                                        MessageBox.Show(ee.Message);
                                    }
                                    catch (NotSupportedException ee)
                                    {
                                        MessageBox.Show(ee.Message);
                                    }

                                }
                            }
                        }
                        catch
                        {
                            return;
                        }
                    }
                }
            }
            catch
            {
                return;
            }

        }


    }
}
