using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace Utils.FileHandle
{
    public class UploadFileImage
    {
        /// <summary>
        /// 图片最大宽度
        /// </summary>
        private int _imgmaxwidth;
        /// <summary>
        /// 图片最大高度
        /// </summary>
        private int _imgmaxheight;
        /// <summary>
        /// 图片水印类型
        /// </summary>
        private int _watermarktype;
        /// <summary>
        /// 附件上传类型
        /// </summary>
        private string _fileextension;
        /// <summary>
        /// 图片上传大小
        /// </summary>
        private int _imgsize;
        /// <summary>
        /// 文件上传大小
        /// </summary>
        private int _attachsize;
        /// <summary>
        /// 缩略图宽度
        /// </summary>
        private int _thumbnailwidth;
        /// <summary>
        /// 缩略图高度
        /// </summary>
        private int _thumbnailheight;
        /// <summary>
        /// 图片水印文字
        /// </summary>
        private string _watermarktext;
        /// <summary>
        /// 图片水印位置
        /// </summary>
        private int _watermarkposition;
        /// <summary>
        /// 图片生成质量
        /// </summary>
        private int _watermarkimgquality;
        /// <summary>
        /// 文字字体
        /// </summary>
        private string _watermarkfont;
        /// <summary>
        /// 文字大小
        /// </summary>
        private int _watermarkfontsize;
        /// <summary>
        /// 图片水印文件
        /// </summary>
        private string _watermarkpic;
        /// <summary>
        /// 水印透明度
        /// </summary>
        private int _watermarktransparency;
        public UploadFileImage()
        {
            _fileextension = "";
            _attachsize = 0;
            _imgsize = 5*1024;
            _imgmaxheight = 0;
            _imgmaxwidth = 0;
            _thumbnailheight = 0;
            _thumbnailwidth = 0;
            _watermarktype = 0;
            _watermarkposition = 9;
            _watermarkimgquality = 80;
            _watermarkpic = "";
            _watermarktransparency = 10;
            _watermarktext = "";
            _watermarkfont = "";
            _watermarkfontsize = 12;
        }
        /// <summary>
        /// 图片最大高度
        /// </summary>
        public int imgmaxheight
        {
            set { _imgmaxheight = value; }
            get { return _imgmaxheight; }
        }
        /// <summary>
        /// 图片最大宽度
        /// </summary>
        public int imgmaxwidth
        {
            set { _imgmaxwidth = value; }
            get { return _imgmaxwidth; }
        }
        /// <summary>
        /// 图片水印类型
        /// </summary>
        public int watermarktype
        {
            set { _watermarktype = value; }
            get { return _watermarktype; }
        }
        /// <summary>
        /// 附件上传类型
        /// </summary>
        public string fileextension
        {
            set { _fileextension = value; }
            get { return _fileextension; }
        }
        /// <summary>
        /// 图片上传大小
        /// </summary>
        public int imgsize
        {

            set { _imgsize = value; }
            get { return _imgsize; }
        }
        /// <summary>
        /// 文件上传大小
        /// </summary>
        public int attachsize
        {
            set { _attachsize = value; }
            get { return _attachsize; }
        }
        /// <summary>
        /// 缩略图宽度
        /// </summary>
        public int thumbnailwidth
        {
            set { _thumbnailwidth = value; }
            get { return _thumbnailwidth; }
        }
        /// <summary>
        /// 缩略图高度
        /// </summary>
        public int thumbnailheight
        {
            set { _thumbnailheight = value; }
            get { return _thumbnailheight; }
        }
        /// <summary>
        /// 图片水印文字
        /// </summary>
        public string watermarktext
        {
            set { _watermarktext = value; }
            get { return _watermarktext; }
        }
        /// <summary>
        /// 图片水印位置
        /// </summary>
        public int watermarkposition
        {
            set { _watermarkposition = value; }
            get { return _watermarkposition; }
        }
        /// <summary>
        /// 图片生成质量
        /// </summary>
        public int watermarkimgquality
        {
            set { _watermarkimgquality = value; }
            get { return _watermarkimgquality; }
        }
        /// <summary>
        /// 文字字体
        /// </summary>
        public string watermarkfont
        {
            set { _watermarkfont = value; }
            get { return _watermarkfont; }
        }
        /// <summary>
        /// 文字大小
        /// </summary>
        public int watermarkfontsize
        {
            set { _watermarkfontsize = value; }
            get { return _watermarkfontsize; }
        }
        /// <summary>
        /// 图片水印文件
        /// </summary>
        public string watermarkpic
        {
            set { _watermarkpic = value; }
            get { return _watermarkpic; }
        }
        /// <summary>
        /// 水印透明度
        /// </summary>
        public int watermarktransparency
        {
            set { _watermarktransparency = value; }
            get { return _watermarktransparency; }
        }
        /// <summary>
        /// 裁剪图片并保存
        /// </summary>
        public bool cropSaveAs(string fileName, string newFileName, int maxWidth, int maxHeight, int cropWidth, int cropHeight, int X, int Y)
        {
            string fileExt = Utils.GetFileExt(fileName); //文件扩展名，不含“.”
            if (!IsImage(fileExt))
            {
                return false;
            }
            string newFileDir = Utils.GetMapPath(newFileName.Substring(0, newFileName.LastIndexOf(@"/") + 1));
            //检查是否有该路径，没有则创建
            if (!Directory.Exists(newFileDir))
            {
                Directory.CreateDirectory(newFileDir);
            }
            try
            {
                string fileFullPath = Utils.GetMapPath(fileName);
                string toFileFullPath = Utils.GetMapPath(newFileName);
                return Thumbnail.MakeThumbnailImage(fileFullPath, toFileFullPath, 180, 180, cropWidth, cropHeight, X, Y);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 文件上传方法
        /// </summary>
        /// <param name="postedFile">文件流</param>
        /// <param name="isThumbnail">是否生成缩略图</param>
        /// <param name="isWater">是否打水印</param>
        ///  <param name="path">上传相对路径</param>
        /// <returns>上传后文件信息</returns>
        public string fileSaveAs(HttpPostedFileBase postedFile, bool isThumbnail, bool isWater, string path)
        {
            try
            {
                string fileExt = Utils.GetFileExt(postedFile.FileName); //文件扩展名，不含“.”
                int fileSize = postedFile.ContentLength; //获得文件大小，以字节为单位
                string fileName = postedFile.FileName.Substring(postedFile.FileName.LastIndexOf(@"\") + 1); //取得原文件名
                string newFileName = Utils.GetRamCode() + "." + fileExt; //随机生成新的文件名
                string newThumbnailFileName = "suweiyun" + newFileName; //随机生成缩略图文件名
                string upLoadPath = GetUpLoadPath(path, 1); //上传目录相对路径
                string fullUpLoadPath = Utils.GetMapPath(upLoadPath); //上传目录的物理路径
                string newFilePath = upLoadPath + newFileName; //上传后的路径
                string newThumbnailPath = upLoadPath + newThumbnailFileName; //上传后的缩略图路径

                //检查文件扩展名是否合法
                if (!IsImage(fileExt))
                {
                    return "{\"status\": 0, \"msg\": \"不允许上传" + fileExt + "类型的文件！\"}";
                }
                //检查文件大小是否合法
                if (!CheckFileSize(fileExt, fileSize))
                {
                    return "{\"status\": 0, \"msg\": \"文件超过限制的大小啦！\"}";
                }
                //检查上传的物理路径是否存在，不存在则创建
                if (!Directory.Exists(fullUpLoadPath))
                {
                    Directory.CreateDirectory(fullUpLoadPath);
                }

                //保存文件
                postedFile.SaveAs(fullUpLoadPath + newFileName);
                //如果是图片，检查图片是否超出最大尺寸，是则裁剪
                if (IsImage(fileExt) && (imgmaxheight > 0 || imgmaxwidth > 0))
                {
                    Thumbnail.MakeThumbnailImage(fullUpLoadPath + newFileName, fullUpLoadPath + newFileName,
                        this.imgmaxwidth, this.imgmaxheight);
                }
                //如果是图片，检查是否需要生成缩略图，是则生成
                if (IsImage(fileExt) && isThumbnail && this.thumbnailwidth > 0 && this.thumbnailheight > 0)
                {
                    Thumbnail.MakeThumbnailImage(fullUpLoadPath + newFileName, fullUpLoadPath + newThumbnailFileName,
                        this.thumbnailwidth, this.thumbnailheight, "Cut");
                }
                //如果是图片，检查是否需要打水印
                if (IsWaterMark(fileExt) && isWater)
                {
                    switch (this.watermarktype)
                    {
                        case 1:
                            WaterMark.AddImageSignText(newFilePath, newFilePath,
                                this.watermarktext, this.watermarkposition,
                                this.watermarkimgquality, this.watermarkfont, this.watermarkfontsize);
                            break;
                        case 2:
                            WaterMark.AddImageSignPic(newFilePath, newFilePath,
                                this.watermarkpic, this.watermarkposition,
                                this.watermarkimgquality, this.watermarktransparency);
                            break;
                    }
                }
                //处理完毕，返回JOSN格式的文件信息
                return "{\"status\": 1, \"msg\": \"上传文件成功！\", \"name\": \""
                    + fileName + "\", \"path\": \"" + newFilePath + "\", \"thumb\": \""
                    + newThumbnailPath + "\", \"size\": " + fileSize + ", \"ext\": \"" + fileExt + "\"}";
            }
            catch
            {
                return "{\"status\": 0, \"msg\": \"上传过程中发生意外错误！\"}";
            }
        }

        #region 私有方法

        /// <summary>
        /// 返回上传目录相对路径
        /// </summary>
        /// <param name="fileName">上传文件名</param>
        /// <param name="filesave">保存方式</param>
        private string GetUpLoadPath(string path, int filesave)
        {

            switch (filesave)
            {
                case 1: //按年月日每天一个文件夹
                    path += DateTime.Now.ToString("yyyyMMdd");
                    break;
                default: //按年月/日存入不同的文件夹
                    path += DateTime.Now.ToString("yyyyMM") + "/" + DateTime.Now.ToString("dd");
                    break;
            }
            return path + "/";


        }

        /// <summary>
        /// 是否需要打水印
        /// </summary>
        /// <param name="_fileExt">文件扩展名，不含“.”</param>
        private bool IsWaterMark(string _fileExt)
        {
            //判断是否开启水印
            if (watermarktype > 0)
            {
                //判断是否可以打水印的图片类型
                ArrayList al = new ArrayList();
                al.Add("bmp");
                al.Add("jpeg");
                al.Add("jpg");
                al.Add("png");
                if (al.Contains(_fileExt.ToLower()))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// 是否为图片文件
        /// </summary>
        /// <param name="_fileExt">文件扩展名，不含“.”</param>
        private bool IsImage(string _fileExt)
        {
            ArrayList al = new ArrayList();
            al.Add("bmp");
            al.Add("jpeg");
            al.Add("jpg");
            al.Add("gif");
            al.Add("png");
            if (al.Contains(_fileExt.ToLower()))
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// 检查是否为合法的上传文件
        /// </summary>
        private bool CheckFileExt(string _fileExt)
        {
            //检查危险文件
            string[] excExt = { "asp", "aspx", "php", "jsp", "htm", "html" };
            for (int i = 0; i < excExt.Length; i++)
            {
                if (excExt[i].ToLower() == _fileExt.ToLower())
                {
                    return false;
                }
            }
            //检查合法文件
            string[] allowExt = fileextension.Split(',');
            for (int i = 0; i < allowExt.Length; i++)
            {
                if (allowExt[i].ToLower() == _fileExt.ToLower())
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// 检查文件大小是否合法
        /// </summary>
        /// <param name="_fileExt">文件扩展名，不含“.”</param>
        /// <param name="_fileSize">文件大小(B)</param>
        private bool CheckFileSize(string _fileExt, int _fileSize)
        {
            //判断是否为图片文件
            if (IsImage(_fileExt))
            {
                if (this.imgsize > 0 && _fileSize > this.imgsize * 1024)
                {
                    return false;
                }
            }
            else
            {
                if (this.attachsize > 0 && _fileSize > this.attachsize * 1024)
                {
                    return false;
                }
            }
            return true;
        }

        #endregion

    }
}

