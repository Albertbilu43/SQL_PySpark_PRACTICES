# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC En Python, todas las clases tienen un constructor, incluso si tú no lo escribes de forma explícita.Aquí te explico la diferencia real entre definir uno tú mismo ( __ init __ ) o dejar que Python use el constructor por defecto.
# MAGIC
# MAGIC #### La Diferencia Principal
# MAGIC
# MAGIC - Sin constructor explícito: La clase usa un constructor vacío por defecto. 
# MAGIC     No puedes pasar datos únicos al crear el objeto. Todos los objetos nacen idénticos.
# MAGIC - Con constructor explícito ( __ init __ ): Puedes pasar datos personalizados al momento de crear el objeto. 
# MAGIC     Cada objeto puede tener valores diferentes desde el inicio.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Clase SIN constructor explícito
# MAGIC Si no escribes el método  __ init __ , Python crea un constructor básico en segundo plano que no acepta argumentos.
# MAGIC
# MAGIC ##### Example

# COMMAND ----------

class PersonaSinConstructor:
    especie = "Humano"  # Atributo compartido por todos


# Creación del objeto (no puedes pasarle datos entre los paréntesis)
usuario1 = PersonaSinConstructor()
usuario2 = PersonaSinConstructor()

# Ambos objetos son idénticos al nacer
print(usuario1.especie)  # Resultado: Humano
print(usuario2.especie)  # Resultado: Humano


# COMMAND ----------

# MAGIC %md
# MAGIC - Uso ideal: Clases que solo agrupan funciones (métodos) o que manejan datos fijos que nunca cambian entre un objeto y otro

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Clase CON constructor explícito ( __ init __ )
# MAGIC Al definir  __ init __ , obligas a que se proporcionen datos específicos cada vez que se crea un nuevo objeto

# COMMAND ----------


class PersonaConConstructor:

    def __init__(self, nombre, edad):
        self.nombre = nombre  # Atributo único de cada objeto
        self.edad = edad  # Atributo único de cada objeto

# Creación del objeto pasando datos personalizados
usuario1 = PersonaConConstructor("Ana", 25)
usuario2 = PersonaConConstructor("Luis", 30)

# Cada objeto tiene su propia información
print(usuario1.nombre)  # Resultado: Ana
print(usuario2.nombre)  # Resultado: Luis


# COMMAND ----------

# MAGIC %md
# MAGIC - Uso ideal: El 90% de los casos en programación. Se usa cuando necesitas que cada objeto tenga sus propias características (como usuarios con nombres diferentes, productos con distintos precios, etc.).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Resumen de diferencias
# MAGIC - Sintaxis: Sin  __ init __  vs Con  __ init __ (self, ...).
# MAGIC - Parámetros: Paréntesis vacíos () vs Paréntesis con datos ("Ana", 25).
# MAGIC - Estado inicial: Objetos idénticos vs Objetos personalizados. 
# MAGIC - Flexibilidad: Muy baja vs Muy alta.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

class Animal:
    def walk(self):
        print("I am walking")

class Bird(Animal):
    def fly(self):
        print("I am flying")

    def sing(self):
        print("I am singing")

if __name__ == "__main__":
    bird = Bird()
    bird.walk()
    bird.fly()
    bird.sing()