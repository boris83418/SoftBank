import sys
import logging
import os
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox, QPushButton
from PyQt5.QtGui import QPixmap, QBrush, QColor
from PyQt5.QtCore import QThread, pyqtSignal
from softbankapp import Ui_MainWindow
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

# 主視窗類別
class Main(QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi(self)  # 設定 UI
        self.bind_events()  # 綁定按鈕事件
        self.calculate_stock_thread = None
        self.set_background_image()  # 設定背景圖片
        self.setFixedSize(self.size())  # 設定視窗大小不可調整

    def set_background_image(self):
        """設定背景圖片"""
        try:
            # 使用 PyInstaller 的資源路徑
            if getattr(sys, 'frozen', False):
                # 打包後的路徑
                base_path = sys._MEIPASS
            else:
                # 開發環境路徑
                base_path = os.path.dirname(os.path.abspath(__file__))
            
            # 組合圖片完整路徑
            img_path = os.path.join(base_path, "Pic", "delta9.jpg")
            
            # 詳細除錯資訊
            print(f"嘗試載入圖片路徑: {img_path}")
            print(f"基礎路徑: {base_path}")
            print(f"路徑是否存在: {os.path.exists(img_path)}")
            
            # 載入圖片
            pixmap = QPixmap(img_path)
            if pixmap.isNull():
                print(f"圖片載入失敗: {img_path}")
                return
            
            # 調整圖片大小
            scaled_pixmap = pixmap.scaled(745, 365)
            p = self.palette()
            p.setBrush(self.backgroundRole(), QBrush(scaled_pixmap))
            self.setPalette(p)
        
        except Exception as e:
            print(f"設置背景圖片時發生錯誤: {e}")
            import traceback
            traceback.print_exc()

    def bind_events(self):
        """綁定按鈕事件"""
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

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = Main()
    window.show()
    sys.exit(app.exec_())