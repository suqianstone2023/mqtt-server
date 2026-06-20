import json
import math
import struct
import time
import threading
import queue
import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
import paho.mqtt.client as mqtt
import tkinter as tk
from tkinter import ttk, scrolledtext


# ================= 日志队列处理器 =================
class QueueLogHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            self.log_queue.put_nowait(
                self.format(record)
            )
        except queue.Full:
            pass


# ================= 核心服务类 =================
_SHUTDOWN_SENTINEL = object()


class DataPersistService:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.carrier_map = {1: "联通", 2: "电信", 3: "移动"}
        self.data_queue = queue.Queue(maxsize=20000)

        self.conn = None
        self.cur = None
        self.client = None
        self._is_running = False
        self.db_writer_thread = None

    def start(self):
        if self._is_running:
            self.logger.warning("服务已在运行中")
            return True

        self._is_running = True
        self.logger.info("正在初始化服务...")

        try:
            self._connect_db()
        except Exception as e:
            self.logger.error(f"数据库连接失败，服务未启动: {e}")
            self._is_running = False
            return False

        self.db_writer_thread = threading.Thread(target=self._db_writer_loop, daemon=True)
        self.db_writer_thread.start()

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)

        self.logger.info(f"正在连接MQTT Broker: {self.config['broker']}...")
        try:
            self.client.connect(self.config["broker"], int(self.config["port"]), 60)
            self.client.loop_start()
            return True
        except Exception as e:
            self.logger.error(f"MQTT连接失败: {e}")
            self.stop()
            return False

    def stop(self):
        if not self._is_running:
            return
        self._is_running = False
        self.logger.info("正在停止服务...")

        if self.client:
            self.client.disconnect()

        if self.db_writer_thread:
            self.logger.info("发送停止信号给写库线程...")
            # 优化：确保哨兵进入队列，限制最多重试30次(约3秒)，并加入微睡眠防止CPU空转
            inserted = False
            retry_count = 0

            while not inserted and retry_count < 30:
                try:
                    self.data_queue.put_nowait(_SHUTDOWN_SENTINEL)
                    inserted = True
                except queue.Full:
                    try:
                        self.data_queue.get_nowait()
                        self.logger.warning("停止服务时队列已满，丢弃1条旧数据以插入退出信号")
                    except queue.Empty:
                        # 队列突然空了却还是 put 失败？极小概率事件，直接跳出
                        break
                    # 防止写库线程彻底卡死时，主线程在这里高频空转消耗 CPU
                    time.sleep(0.1)
                retry_count += 1

            if not inserted:
                self.logger.error("无法将停止信号插入队列，写库线程可能已僵死！")

            self.db_writer_thread.join(timeout=5)

        if self.client:
            self.client.loop_stop()

        # 彻底释放连接资源
        try:
            if self.cur: self.cur.close()
        except Exception:
            pass
        try:
            if self.conn and not self.conn.closed: self.conn.close()
        except Exception:
            pass
        self.logger.info("数据库连接已关闭")
        self.logger.info("服务已完全停止")

    def _connect_db(self):
        # 重连前彻底清理旧游标和连接
        try:
            if self.cur: self.cur.close()
        except Exception:
            pass
        try:
            if self.conn and not self.conn.closed: self.conn.close()
        except Exception:
            pass

        self.conn = psycopg2.connect(
            connect_timeout=5,
            **self.config["db_config"]
        )
        self.cur = self.conn.cursor()
        self.logger.info("PostgreSQL连接成功")

    def parse_meter(self, raw):
        try:
            raw = raw.strip().replace(" ", "").upper()

            # 兼容带校验码的报文。只要长度>=62，就截取中间核心的62个字符(前6后56)
            if len(raw) not in (62, 66):
                self.logger.warning(
                    f"数据包长度异常(仅支持62或66, 实际{len(raw)}): {raw}"
                )
                return None

            # 提前验证 HEX，避免高频异常捕获开销
            valid_hex = set('0123456789ABCDEF')
            if not all(c in valid_hex for c in raw):
                self.logger.warning(f"数据包含非十六进制字符: {raw}")
                return None

            meter_addr = int(raw[0:2], 16)
            carrier = self.carrier_map.get(meter_addr, "未配置")

            # 精确截取 6 到 62 位，忽略 62 位之后的校验码
            payload = raw[6:62]

            values = []
            for i in range(0, 56, 8):
                block = payload[i:i + 8]
                value = struct.unpack(
                    ">f",
                    bytes.fromhex(block)
                )[0]

                if not math.isfinite(value):
                    self.logger.warning(
                        f"非法浮点数: {block}"
                    )
                    return None

                values.append(
                    round(value, 3)
                )

            if len(values) != 7:
                return None

            return {
                "meter_addr": meter_addr, "carrier": carrier,
                "voltage": values[0], "current": values[1], "power": values[2],
                "power_factor": values[3], "frequency": values[4],
                "energy": values[5], "load_rate": int(values[6])
            }
        except Exception as e:
            self.logger.error(f"解析异常: {repr(e)}")
            return None

    def _db_writer_loop(self):
        batch = []
        last_flush_time = None
        batch_size = max(1, int(self.config["batch_size"]))
        flush_interval = max(1, int(self.config["flush_interval"]))
        pending_retry_batch = None

        while True:
            # 只要主线程叫停 (_is_running=False)，立刻丢弃重试任务，强制退出！彻底杜绝僵尸线程
            if not self._is_running:
                if pending_retry_batch:
                    self.logger.error("服务已停止，丢弃无法写入的重试批次数据！")
                    pending_retry_batch = None
                if batch:
                    if not self._execute_db_write(batch):
                        self.logger.error("退出前最后一次写入失败，数据丢弃！")
                break

            # 优先处理需要重试的失败批次
            if pending_retry_batch:
                success = self._execute_db_write(pending_retry_batch)
                if success:
                    pending_retry_batch = None
                else:
                    time.sleep(5)
                    continue

            # 从队列获取新数据
            try:
                item = self.data_queue.get(timeout=1)
                if item is _SHUTDOWN_SENTINEL:
                    if batch:
                        self._execute_db_write(batch)
                    break
                batch.append(item)

                if len(batch) == 1:
                    last_flush_time = time.time()
            except queue.Empty:
                pass

            # 检查是否达到批量写入条件
            current_time = time.time()
            if len(batch) >= batch_size or (
                    batch
                    and last_flush_time is not None
                    and current_time - last_flush_time >= flush_interval
            ):
                success = self._execute_db_write(batch)
                if success:
                    batch.clear()
                    last_flush_time = current_time
                else:
                    pending_retry_batch = batch[:]
                    batch.clear()
                    last_flush_time = current_time

    def _execute_db_write(self, rows):
        if not self.conn or self.conn.closed:
            self.logger.error("数据库连接已断开，尝试重连...")
            try:
                self._connect_db()
            except Exception:
                return False

        try:
            sql = """
                  INSERT INTO meter_data
                  (ts, gateway, carrier, meter_addr, voltage, current, power, power_factor, frequency, energy, \
                   load_rate)
                  VALUES %s \
                  """
            start_time = time.time()
            # 指定 page_size，避免 psycopg2 默认分片导致的网络交互浪费
            execute_values(
                self.cur,
                sql,
                rows,
                page_size=len(rows)
            )
            self.conn.commit()
            cost_time = (time.time() - start_time) * 1000
            self.logger.info(f"批量写入 {len(rows)} 条成功，耗时: {cost_time:.2f} 毫秒")
            return True
        except Exception as e:
            self.logger.error(f"批量写入失败: {e}")
            try:
                if self.conn: self.conn.rollback()
                if self.conn: self.conn.close()
            except Exception:
                pass
            self.conn = None
            self.cur = None
            return False

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self.logger.info(f"MQTT连接成功，订阅主题: {self.config['topic']}")
            result, mid = client.subscribe(
                self.config["topic"]
            )

            if result == mqtt.MQTT_ERR_SUCCESS:
                self.logger.info(
                    f"主题订阅成功: {self.config['topic']}"
                )
            else:
                self.logger.error(
                    f"主题订阅失败: {result}"
                )
        else:
            self.logger.error(f"MQTT连接失败，错误码: {reason_code}")

    def _on_message(self, client, userdata, msg):
        if not self._is_running: return

        try:
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
            gateway = data.get("MAC", "Unknown")
            receive_time = datetime.now(timezone.utc)

            for i in range(1, 11):
                key = f"k{i}"
                raw = data.get(key)
                if not raw: continue
                meter = self.parse_meter(raw)
                if meter is None: continue

                row = (
                    receive_time, gateway, meter["carrier"], meter["meter_addr"],
                    meter["voltage"], meter["current"], meter["power"],
                    meter["power_factor"], meter["frequency"], meter["energy"],
                    meter["load_rate"]
                )

                try:
                    self.data_queue.put(row, block=True, timeout=2)
                except queue.Full:
                    self.logger.error("数据队列已满(超过2秒)，被迫丢弃当前数据！")

        except Exception as e:
            self.logger.error(f"消息处理失败: {e}")

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        if self._is_running:
            self.logger.warning(f"MQTT连接断开，准备重连...")


# ================= GUI 界面类 =================
class AppGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("MQTT 数据采集与持久化工具 v0.99")
        self.root.geometry("750x650")

        self.service = None
        self.log_queue = queue.Queue(
            maxsize=5000
        )

        self.setup_logger()
        self.setup_ui()
        self.poll_log_queue()

    def setup_logger(self):
        self.logger = logging.getLogger("MqttApp")
        self.logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(console_handler)

        ui_handler = QueueLogHandler(self.log_queue)
        ui_handler.setLevel(logging.DEBUG)
        ui_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(ui_handler)

    def setup_ui(self):
        config_frame = ttk.LabelFrame(self.root, text="环境配置", padding=10)
        config_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(config_frame, text="MQTT Broker:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.entry_broker = ttk.Entry(config_frame, width=15)
        self.entry_broker.insert(0, "192.168.3.90")
        self.entry_broker.grid(row=0, column=1, padx=5)

        ttk.Label(config_frame, text="端口:").grid(row=0, column=2, sticky=tk.W)
        self.entry_mqtt_port = ttk.Entry(config_frame, width=6)
        self.entry_mqtt_port.insert(0, "1883")
        self.entry_mqtt_port.grid(row=0, column=3)

        ttk.Label(config_frame, text="主题:").grid(row=0, column=4, sticky=tk.W)
        self.entry_topic = ttk.Entry(config_frame, width=10)
        self.entry_topic.insert(0, "pTopic")
        self.entry_topic.grid(row=0, column=5, padx=5)

        ttk.Label(config_frame, text="数据库IP:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.entry_db_host = ttk.Entry(config_frame, width=15)
        self.entry_db_host.insert(0, "127.0.0.1")
        self.entry_db_host.grid(row=1, column=1, padx=5)

        ttk.Label(config_frame, text="端口:").grid(row=1, column=2, sticky=tk.W)
        self.entry_db_port = ttk.Entry(config_frame, width=6)
        self.entry_db_port.insert(0, "5432")
        self.entry_db_port.grid(row=1, column=3)

        ttk.Label(config_frame, text="库名:").grid(row=1, column=4, sticky=tk.W)
        self.entry_db_name = ttk.Entry(config_frame, width=10)
        self.entry_db_name.insert(0, "ems")
        self.entry_db_name.grid(row=1, column=5, padx=5)

        ttk.Label(config_frame, text="用户:").grid(row=2, column=0, sticky=tk.W, pady=2)
        self.entry_db_user = ttk.Entry(config_frame, width=15)
        self.entry_db_user.insert(0, "postgres")
        self.entry_db_user.grid(row=2, column=1, padx=5)

        ttk.Label(config_frame, text="密码:").grid(row=2, column=2, sticky=tk.W)
        self.entry_db_pass = ttk.Entry(config_frame, width=10, show="*")
        self.entry_db_pass.insert(0, "testpasswd")
        self.entry_db_pass.grid(row=2, column=3)

        ttk.Label(config_frame, text="批量大小:").grid(row=2, column=4, sticky=tk.W)
        self.entry_batch_size = ttk.Entry(config_frame, width=5)
        self.entry_batch_size.insert(0, "1000")
        self.entry_batch_size.grid(row=2, column=5)

        ttk.Label(config_frame, text="刷库间隔(秒):").grid(row=3, column=0, sticky=tk.W, pady=2)
        self.entry_flush_interval = ttk.Entry(config_frame, width=5)
        self.entry_flush_interval.insert(0, "3")
        self.entry_flush_interval.grid(row=3, column=1, sticky=tk.W)

        btn_frame = ttk.Frame(self.root)
        btn_frame.pack(fill=tk.X, padx=10, pady=5)

        self.btn_start = ttk.Button(btn_frame, text="启动服务", command=self.start_service)
        self.btn_start.pack(side=tk.LEFT, padx=5)

        self.btn_stop = ttk.Button(btn_frame, text="停止服务", command=self.stop_service, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=5)

        log_frame = ttk.LabelFrame(self.root, text="运行日志 / 调试信息", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=("Consolas", 9), bg="#f5f5f5")
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # 修复5：提前定义并配置 tag，避免行号偏移导致的颜色错位
        self.log_text.tag_config("error", foreground="red")
        self.log_text.tag_config("info", foreground="green")
        self.log_text.config(state=tk.DISABLED)

    def get_config(self):
        return {
            "broker": self.entry_broker.get(),
            "port": self.entry_mqtt_port.get(),
            "topic": self.entry_topic.get(),
            "batch_size": self.entry_batch_size.get(),
            "flush_interval": self.entry_flush_interval.get(),
            "db_config": {
                "host": self.entry_db_host.get(),
                "port": self.entry_db_port.get(),
                "database": self.entry_db_name.get(),
                "user": self.entry_db_user.get(),
                "password": self.entry_db_pass.get()
            }
        }

    def toggle_ui_state(self, is_running):
        state_normal = tk.DISABLED if is_running else tk.NORMAL
        state_disabled = tk.NORMAL if is_running else tk.DISABLED

        for entry in [self.entry_broker, self.entry_mqtt_port, self.entry_topic,
                      self.entry_db_host, self.entry_db_port, self.entry_db_name,
                      self.entry_db_user, self.entry_db_pass, self.entry_batch_size,
                      self.entry_flush_interval]:
            entry.config(state=state_normal)

        self.btn_start.config(state=state_normal)
        self.btn_stop.config(state=state_disabled)

    def start_service(self):
        config = self.get_config()
        self.service = DataPersistService(config, self.logger)

        self.toggle_ui_state(True)

        threading.Thread(
            target=self._start_service_worker,
            daemon=True
        ).start()

    def _start_service_worker(self):
        success = self.service.start()

        self.root.after(
            0,
            lambda: self._handle_start_result(success)
        )

    def _handle_start_result(self, success):
        if success:
            self.logger.info("服务启动成功，可以开始接收数据。")
        else:
            if self.service:
                self.service.stop()
                self.service = None

            self.logger.error("服务启动失败，请检查配置和网络后重试。")
            self.toggle_ui_state(False)

    def stop_service(self):
        if self.service:
            self.service.stop()
            self.service = None
        self.toggle_ui_state(False)

    def poll_log_queue(self):
        while not self.log_queue.empty():
            msg = self.log_queue.get_nowait()
            self.log_text.config(state=tk.NORMAL)

            # 修复5：在 insert 时直接应用 tag，确保颜色精准对应每行
            tag = None
            if "ERROR" in msg or "失败" in msg:
                tag = "error"
            elif "成功" in msg or "INFO" in msg:
                tag = "info"

            self.log_text.insert(tk.END, msg + "\n", tag)

            # 精准计算行数并清理旧日志
            line_count = int(self.log_text.index('end-1c').split('.')[0])
            if line_count > 1000:
                # 删除最早的 200 行
                self.log_text.delete('1.0', f'{line_count - 800}.0')

            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)

        self.root.after(100, self.poll_log_queue)

    def on_closing(self):
        if self.service:
            self.service.stop()
        self.root.destroy()


if __name__ == "__main__":
    try:
        root = tk.Tk()
        app = AppGUI(root)
        root.protocol("WM_DELETE_WINDOW", app.on_closing)
        root.mainloop()
    except Exception as e:
        import traceback
        from tkinter import messagebox

        tk.Tk().withdraw()
        messagebox.showerror("启动致命错误", f"程序启动失败:\n{traceback.format_exc()}")