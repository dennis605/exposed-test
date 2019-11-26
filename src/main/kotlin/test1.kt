import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

object Bewohner : Table() {
    val id = varchar("id", 10).primaryKey() // Column<String>
    val name = varchar("name", length = 50) // Column<String>
    val cityId = (integer("city_id") references Kollege.id).nullable() // Column<Int?>
}

object Kollege : Table() {
    val id = integer("id").autoIncrement().primaryKey() // Column<Int>
    val name = varchar("name", 50) // Column<String>
}

fun main(args: Array<String>) {
    Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")

    val choice = readLine()

    transaction {
        SchemaUtils.create (Kollege, Bewohner)

        val saintPetersburgId = Kollege.insert {
            it[name] = "$choice"
        } get Kollege.id

        val munichId = Kollege.insert {
            it[name] = "Munich"
        } get Kollege.id

        Kollege.insert {
            it[name] = "Prague"
        }

        Bewohner.insert {
            it[id] = "andrey"
            it[name] = "Andrey"
            it[cityId] = saintPetersburgId
        }

        Bewohner.insert {
            it[id] = "sergey"
            it[name] = "Sergey"
            it[cityId] = munichId
        }

        Bewohner.insert {
            it[id] = "eugene"
            it[name] = "Eugene"
            it[cityId] = munichId
        }

        Bewohner.insert {
            it[id] = "alex"
            it[name] = "Alex"
            it[cityId] = null
        }

        Bewohner.insert {
            it[id] = "smth"
            it[name] = "Something"
            it[cityId] = null
        }

        Bewohner.update({Bewohner.id eq "alex"}) {
            it[name] = "Alexey"
        }

        Bewohner.deleteWhere{Bewohner.name like "%thing"}

        println("All cities:")

        for (city in Kollege.selectAll()) {
            println("${city[Kollege.id]}: ${city[Kollege.name]}")
        }

        println("Manual join:")
        (Bewohner innerJoin Kollege).slice(Bewohner.name, Kollege.name).
            select {(Bewohner.id.eq("andrey") or Bewohner.name.eq("Sergey")) and
                    Bewohner.id.eq("sergey") and Bewohner.cityId.eq(Kollege.id)}.forEach {
            println("${it[Bewohner.name]} lives in ${it[Kollege.name]}")
        }

        println("Join with foreign key:")


        (Bewohner innerJoin Kollege).slice(Bewohner.name, Bewohner.cityId, Kollege.name).
            select {Kollege.name.eq("St. Petersburg") or Bewohner.cityId.isNull()}.forEach {
            if (it[Bewohner.cityId] != null) {
                println("${it[Bewohner.name]} lives in ${it[Kollege.name]}")
            }
            else {
                println("${it[Bewohner.name]} lives nowhere")
            }
        }

        println("Functions and group by:")

        ((Kollege innerJoin Bewohner).slice(Kollege.name, Bewohner.id.count()).selectAll().groupBy(Kollege.name)).forEach {
            val cityName = it[Kollege.name]
            val userCount = it[Bewohner.id.count()]

            if (userCount > 0) {
                println("$userCount user(s) live(s) in $cityName")
            } else {
                println("Nobody lives in $cityName")
            }
        }

        SchemaUtils.drop (Bewohner, Kollege)

    }
}