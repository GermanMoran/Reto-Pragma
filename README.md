# Reto Tecnico

## Descripción

- Este Repositorio tine asociado los artefactos del reto tecnico para el cargo de Data Enginner. El archivo final.py contiene el codigo de la solución

## Herramientas 🛠️

- Python
- Jupyter Notebooks
- Git Hub
- Docker
- Postgres SQL

## Pasos para hacer uso del Repo

1. Clonar el código del repositorio de git
2. Crear un entorno virtual e instalar las dependencias dentro de visual. Si se instala Postgres localmente unicamente se debe actulizar la conexion PGHOST dentro de las variables de entorno (.env)
3. Dentro del terminal ejecutar los siguientes comandos

   ```
   Ingesta de datos: python final.py --chunksize 100
   Ingesta archivo validacion: python final.py --chunksize 100 --validation
   Truncar tablas: python final.py --truncate-tables

   ```

4. Si se desea hacer uso del repo mediante contendores, se debe previamente haber instlado docker en el S.O

5. Copiar la carpeta del repo dentro de carpeta root /WSL
6. Ejecutamos el siguiente comando para lanzar el proyecto

   ```
   docker-compose up -d
   ```

7. Para detener el contenedor ejecutamos el siguiente comando

   ```
   docker-compose down
   ```

8. Para ejecutar la ingesta de los archivos .csv y verificación de estadisticas ejecutar

   ```
    docker-compose exec python-app python final.py --chunksize 100

    docker-compose exec python-app python final.py --chunksize 100 --validation


   ```

9. Verificacion de logs y salidas del script final.py
   ```
   docker logs -f python-app
   ```
