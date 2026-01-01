def leer_datos():
    try:
        edad = int(input("Ingresa tu edad: "))
        estatura = float(input("Ingresa tu estatura (en metros): "))
        print(f"Edad: {edad} años")
        print(f"Estatura: {estatura} m")
        return edad, estatura
    except ValueError:
        print("Por favor ingresa valores numéricos válidos.")
        return None, None

if __name__ == "__main__":
    print("Hello World my name is sarai")
    leer_datos()