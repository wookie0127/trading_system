# main_window.py

import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton
from PyQt5.QtWebEngineWidgets import QWebEngineView
import httpx


class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Trading System")

        # 웹 엔진 뷰 생성
        self.web_view = QWebEngineView(self)
        self.setCentralWidget(self.web_view)
        self.web_view.load(
            "http://localhost:8000/"
        )  # FastAPI 서버가 실행 중인 URL로 설정

        # 매수 버튼 생성
        self.buy_button = QPushButton("Buy", self)
        self.buy_button.setGeometry(100, 50, 100, 50)
        self.buy_button.clicked.connect(self.buy_stock)

    def buy_stock(self):
        # "localhost:8000/buy"로 요청을 보내서 FastAPI 서버에 매수 요청을 전달합니다.
        response = httpx.post(
            "http://localhost:8000/buy/AAPL/10", timeout=300
        )  # 예시로 AAPL 주식을 10주 매수합니다.
        print(response.json())

    def sell_stock(self):
        # "localhost:8000/buy"로 요청을 보내서 FastAPI 서버에 매수 요청을 전달합니다.
        response = httpx.post(
            "http://localhost:8000/sell/AAPL/10", timeout=300
        )  # 예시로 AAPL 주식을 10주 매도합니다.
        print(response.json())


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
