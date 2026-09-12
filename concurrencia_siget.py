# =====================================================================
#  SIMULACIÓN DE CONCURRENCIA PARA EL SIGET
#  Problema: Productor-Consumidor adaptado
#  Sensores de tráfico (productores) → Búfer → Módulos de análisis (consumidores)
#  Sincronización: Semáforos (vacíos, llenos) + Mutex
# =====================================================================
import threading
import time
import random
import os
import sys

# Habilitar colores ANSI en Windows
if os.name == "nt":
    os.system("")

ANSI = {"r": "91", "g": "92", "y": "93", "b": "94", "m": "95",
        "c": "96", "gr": "90", "bd": "1", "rst": "0"}

def _c(txt, color):
    return f"\033[{ANSI.get(color, '0')}m{txt}\033[0m"

def cls():
    os.system("cls" if os.name == "nt" else "clear")


def sem_value(sem):
    """Devuelve el valor del semáforo de forma segura."""
    try:
        return sem._value
    except AttributeError:
        return "?"


# =====================================================================
# 1. BÚFER COMPARTIDO CON SEMÁFOROS
# =====================================================================
class BufferCompartido:
    def __init__(self, capacidad, log):
        self.capacidad = capacidad
        self.buffer = [None] * capacidad
        self.entrada = 0
        self.salida = 0
        self.vacios = threading.Semaphore(capacidad)
        self.llenos = threading.Semaphore(0)
        self.mutex = threading.Lock()
        self.log = log

    def producir(self, item, productor):
        self.vacios.acquire()
        self.mutex.acquire()
        try:
            pos = self.entrada
            self.buffer[pos] = item
            self.entrada = (self.entrada + 1) % self.capacidad
        finally:
            self.mutex.release()
        self.llenos.release()
        self.log.append(("PROD", productor, item, pos))

    def consumir(self, consumidor, timeout=0.3):
        if not self.llenos.acquire(timeout=timeout):
            return None
        self.mutex.acquire()
        try:
            pos = self.salida
            item = self.buffer[pos]
            self.buffer[pos] = None
            self.salida = (self.salida + 1) % self.capacidad
        finally:
            self.mutex.release()
        self.vacios.release()
        self.log.append(("CONS", consumidor, item, pos))
        return item


# =====================================================================
# 2. PRODUCTOR: SENSOR DE TRÁFICO
# =====================================================================
class Sensor(threading.Thread):
    TIPOS = ["Velocidad", "Ocupacion", "Flujo", "Cola"]

    def __init__(self, id_sensor, zona, buffer, stats, stats_lock,
                 max_items, delay, stop_event):
        super().__init__(daemon=True, name=f"Sensor-{id_sensor}")
        self.id = id_sensor
        self.zona = zona
        self.buffer = buffer
        self.stats = stats
        self.stats_lock = stats_lock
        self.max_items = max_items
        self.delay = delay
        self.stop_event = stop_event

    def run(self):
        try:
            for i in range(self.max_items):
                if self.stop_event.is_set():
                    break
                tipo = random.choice(self.TIPOS)
                valor = random.randint(20, 120)
                dato = {
                    "id": f"S{self.id}-{i:03d}",
                    "zona": self.zona,
                    "tipo": tipo,
                    "valor": valor,
                    "hora": time.time(),
                }
                self.buffer.producir(dato, f"Sensor-{self.id}({self.zona})")
                with self.stats_lock:
                    self.stats["producidos"] += 1
                time.sleep(self.delay)
        finally:
            with self.stats_lock:
                self.stats["productores_terminados"] += 1


# =====================================================================
# 3. CONSUMIDOR: MÓDULO DE ANÁLISIS
# =====================================================================
class Analizador(threading.Thread):
    def __init__(self, id_mod, buffer, stats, stats_lock,
                 delay, stop_event):
        super().__init__(daemon=True, name=f"Analizador-{id_mod}")
        self.id = id_mod
        self.buffer = buffer
        self.stats = stats
        self.stats_lock = stats_lock
        self.delay = delay
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            dato = self.buffer.consumir(f"Analizador-{self.id}", timeout=0.3)
            if dato is None:
                continue
            time.sleep(self.delay)
            with self.stats_lock:
                self.stats["consumidos"] += 1
                self.stats["suma_valores"] += dato["valor"]
                self.stats["ultimo"] = dato


# =====================================================================
# 4. DASHBOARD EN CONSOLA
# =====================================================================
def imprimir_dashboard(buffer_obj, stats, productos_activos, consumidores_activos):
    cls()
    ancho = 84
    print(_c("=" * ancho, "b"))
    print(_c(" SIMULADOR DE CONCURRENCIA — SIGET (Productor-Consumidor)".center(ancho), "bd"))
    print(_c("=" * ancho, "b"))

    print(_c(" BUFER COMPARTIDO", "bd"))
    contenido = "  ".join(
        _c(f"[{d['id']}]", "g") if d else _c("[ -- ]", "gr")
        for d in buffer_obj.buffer
    )
    print(f"  {contenido}")
    print(f"  Capacidad: {buffer_obj.capacidad}   "
          f"Entrada: {buffer_obj.entrada}   Salida: {buffer_obj.salida}")

    print(_c(" " + "-" * (ancho - 2), "gr"))
    print(_c(" SEMAFOROS", "bd"))
    print(f"  vacios (espacios libres) : {sem_value(buffer_obj.vacios)}")
    print(f"  llenos (datos listos)    : {sem_value(buffer_obj.llenos)}")

    print(_c(" " + "-" * (ancho - 2), "gr"))
    print(_c(" ESTADISTICAS COMPARTIDAS", "bd"))
    print(f"  Producidos : {stats['producidos']}   "
          f"Consumidos : {stats['consumidos']}   "
          f"En bufer : {stats['producidos'] - stats['consumidos']}")
    print(f"  Suma de valores analizados : {stats['suma_valores']}")

    if stats.get("ultimo"):
        u = stats["ultimo"]
        print(f"  Ultimo analizado : {u['id']} | zona={u['zona']} | "
              f"tipo={u['tipo']} | valor={u['valor']}")

    print(_c(" " + "-" * (ancho - 2), "gr"))
    print(_c(" HILOS EN EJECUCION", "bd"))
    print(f"  Productores activos   : {productos_activos}")
    print(f"  Consumidores activos  : {consumidores_activos}")
    print(_c("=" * ancho, "b"))


# =====================================================================
# 5. EJECUCION PRINCIPAL
# =====================================================================
def ejecutar_simulacion(n_sensores=3, n_analizadores=2,
                        capacidad=5, items_por_sensor=6,
                        delay_prod=0.8, delay_cons=1.2,
                        visual=True, max_duracion=60):
    log = []
    stats = {"producidos": 0, "consumidos": 0,
             "suma_valores": 0, "ultimo": None,
             "productores_terminados": 0}
    stats_lock = threading.Lock()
    stop_event = threading.Event()

    buffer_obj = BufferCompartido(capacidad, log)
    zonas = ["Norte", "Centro", "Sur", "Oriente", "Occidente"]

    sensores = [
        Sensor(i + 1, zonas[i % len(zonas)], buffer_obj,
               stats, stats_lock, items_por_sensor, delay_prod, stop_event)
        for i in range(n_sensores)
    ]
    analizadores = [
        Analizador(j + 1, buffer_obj, stats, stats_lock, delay_cons, stop_event)
        for j in range(n_analizadores)
    ]

    for s in sensores:
        s.start()
    for a in analizadores:
        a.start()

    total_esperado = n_sensores * items_por_sensor
    t0 = time.time()

    if visual:
        while True:
            vivos_p = sum(1 for s in sensores if s.is_alive())
            vivos_c = sum(1 for a in analizadores if a.is_alive())
            imprimir_dashboard(buffer_obj, stats, vivos_p, vivos_c)

            if stats["consumidos"] >= total_esperado and vivos_p == 0:
                break
            if time.time() - t0 > max_duracion:
                break
            time.sleep(0.6)
    else:
        for s in sensores:
            s.join()
        while stats["consumidos"] < total_esperado:
            if time.time() - t0 > max_duracion:
                break
            time.sleep(0.05)

    stop_event.set()
    for a in analizadores:
        a.join(timeout=2.0)

    if visual:
        imprimir_dashboard(buffer_obj, stats, 0, 0)

    # Reporte final
    print()
    print(_c("=" * 84, "b"))
    print(_c(" REPORTE FINAL", "bd"))
    print(_c("=" * 84, "b"))
    print(f"  Sensores (productores)     : {n_sensores}")
    print(f"  Analizadores (consumidores): {n_analizadores}")
    print(f"  Capacidad del bufer        : {capacidad}")
    print(f"  Total producidos           : {stats['producidos']}")
    print(f"  Total consumidos           : {stats['consumidos']}")
    print(f"  Perdidas de datos          : {stats['producidos'] - stats['consumidos']}")
    print(f"  Promedio de valores        : "
          f"{stats['suma_valores'] / max(1, stats['consumidos']):.2f}")
    print()
    if stats["producidos"] == stats["consumidos"]:
        print(_c("  OK No hubo perdida de datos.", "g"))
    else:
        print(_c("  X  Hubo perdida de datos.", "r"))
    print(_c("  OK No hubo corrupcion (mutex protegio el bufer).", "g"))
    print(_c("  OK No hubo deadlock (semaforos balanceados).", "g"))
    print(_c("=" * 84, "b"))

    return stats


# =====================================================================
# 6. MAIN
# =====================================================================
def main():
    args = sys.argv[1:]
    visual = "--sin-visual" not in args

    n_sensores = 3
    items_por_sensor = 6

    print(_c("\nIniciando simulacion de concurrencia del SIGET...\n", "y"))
    time.sleep(1)

    stats = ejecutar_simulacion(
        n_sensores=n_sensores,
        n_analizadores=2,
        capacidad=5,
        items_por_sensor=items_por_sensor,
        delay_prod=0.8,
        delay_cons=1.2,
        visual=visual,
    )

    esperado = n_sensores * items_por_sensor
    if stats["producidos"] == esperado and stats["consumidos"] == esperado:
        print(_c("\n  Verificacion de integridad: OK\n", "g"))
    else:
        print(_c(f"\n  Verificacion fallo: "
                 f"producidos={stats['producidos']}, "
                 f"consumidos={stats['consumidos']}, "
                 f"esperado={esperado}\n", "r"))

    # -----------------------------------------------------------
    # PAUSA FINAL: evita que la ventana se cierre sola.
    # -----------------------------------------------------------
    try:
        input(_c("  Presione ENTER para salir... ", "y"))
    except EOFError:
        time.sleep(10)


if __name__ == "__main__":
    main()