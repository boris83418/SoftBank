import sys
import logging
import os
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox, QPushButton
from PyQt5.QtGui import QPixmap, QBrush, QColor
from PyQt5.QtCore import QThread, pyqtSignal
from softbankapp import Ui_MainWindow
from SoftBank_SummaryTable_Export import export_summarytable_to_excel, connect_to_database
from SoftBank_StockCalculate import main

# 取得當前執行檔的目錄，適用於開發和打包的環境
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if not getattr(sys, 'frozen', False) else sys._MEIPASS

# 設定 log 檔案儲存位置
LOG_PATH = os.path.join(BASE_DIR, "logfile.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 計算庫存的執行緒
class CalculateStockThread(QThread):
    finished = pyqtSignal(bool, str)  # 成功與否，訊息
    def run(self):
        try:
            logging.info("Starting stock calculation script...")
            returncode=main()
            if returncode == 0:
                self.finished.emit(True, "Stock calculation successful!")
                logging.info("Stock calculation successful!")
            else:
                logging.info("Stock calculation failed. Check STDERR for more details.")
        except Exception as e:
            logging.error(f"Error occurred while running the stock calculation script: {e}")
            self.finished.emit(False, str(e)) 
        


# 匯出出貨清單的執行緒
class ExportThread(QThread):
    finished = pyqtSignal(bool, str)

    def __init__(self, conn, output_dir, parent=None):
        super().__init__(parent)
        self.conn = conn  # 資料庫連線
        self.output_dir = output_dir  # 匯出目錄

    def run(self):
        """執行匯出出貨清單的操作"""
        try:
            logging.info("Starting export of shipping list...")
            table_name = "dbo.SoftBankSummaryView"
            export_summarytable_to_excel(self.conn, table_name, self.output_dir)
            self.finished.emit(True, "Shipping list exported successfully!")
        except Exception as e:
            logging.error(f"Export failed: {e}")
            self.finished.emit(False, str(e))


# 主視窗類別
class Main(QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi(self)  # 設定 UI
        self.bind_events()  # 綁定按鈕事件
        self.export_thread = None
        self.calculate_stock_thread = None
        self.set_background_image()  # 設定背景圖片
        self.setFixedSize(self.size())  # 設定視窗大小不可調整

    def set_background_image(self):
        """設定背景圖片"""
        self.setAutoFillBackground(True)
        p = self.palette()
        p.setColor(self.backgroundRole(), QColor(255, 255, 255)) 
        self.setPalette(p)

        img_path = os.path.join(BASE_DIR, "Pic", "delta9.jpg")
        pixmap = QPixmap(img_path)
        pixmap = pixmap.scaled(745, 365)  # 調整圖片大小
        p.setBrush(self.backgroundRole(), QBrush(pixmap))
        self.setPalette(p)

    def bind_events(self):
        """綁定按鈕事件"""
        self.pushButton_2.clicked.connect(self.start_export_thread)
        self.pushButton_7.clicked.connect(self.start_calculatestock)

    def toggle_buttons(self, enable=True, exclude_button=None):
        """啟用或禁用所有按鈕，排除特定按鈕"""
        for button in self.findChildren(QPushButton):
            if button != exclude_button:
                button.setEnabled(enable)

    def start_calculatestock(self):
        """開始計算庫存"""
        logging.info("Start stock calculation...")
        self.pushButton_7.setEnabled(False)
        self.toggle_buttons(enable=False, exclude_button=self.pushButton_7)

        self.calculate_stock_thread = CalculateStockThread()
        self.calculate_stock_thread.finished.connect(self.handle_calculatestock_result)
        self.calculate_stock_thread.start()

    def handle_calculatestock_result(self, success, message):
        """處理庫存計算結果"""
        if success:
            QMessageBox.information(self, "Success", message)
        else:
            QMessageBox.critical(self, "Error", message)
        self.pushButton_7.setEnabled(True)
        self.toggle_buttons(enable=True)
        self.calculate_stock_thread = None

    def start_export_thread(self):
        """開始匯出出貨清單"""
        logging.info("Start exporting shipping list...")
        self.pushButton_2.setEnabled(False)
        self.toggle_buttons(enable=False, exclude_button=self.pushButton_2)

        try:
            conn = connect_to_database('jpdejitdev01', 'ITQAS2')  # 連接資料庫
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Unable to connect to the database: {e}")
            self.pushButton_2.setEnabled(True)
            self.toggle_buttons(enable=True)
            return

        output_dir = r"\\jpdejstcfs01\\STC_share\\JP IT\\STC SBK 仕分けリスト\\IT system\\Report"
        self.export_thread = ExportThread(conn, output_dir)
        self.export_thread.finished.connect(self.handle_export_result)
        self.export_thread.start()

    def handle_export_result(self, success, message):
        """處理匯出結果"""
        if success:
            QMessageBox.information(self, "Success", message)
        else:
            QMessageBox.critical(self, "Error", message)
        self.pushButton_2.setEnabled(True)
        self.toggle_buttons(enable=True)
        self.export_thread = None


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = Main()
    window.show()
    sys.exit(app.exec_())
