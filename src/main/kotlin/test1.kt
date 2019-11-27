import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

// Object Bewohner
object Bewohner : Table() {
    val id = varchar("id", 10).primaryKey() // Column<String>
    val firstName_Bewohner = varchar("name", length = 50) // Column<String>
    val lastName_Bewohner = varchar("name", length = 50) // Column<String>

    val cityId = (integer("city_id") references Kollege.id).nullable() // Column<Int?>
}
//Object Kollege
object Kollege : Table() {
    val id = integer("id").autoIncrement().primaryKey() // Column<Int>
    val firstName_Kollege = varchar("name", 50) // Column<String>
    val lastName_Kollege = varchar("name", 50) // Column<String>

}

fun main(args: Array<String>) {
    Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")

    val choice = readLine()

    transaction {
        SchemaUtils.create (Kollege, Bewohner)

        val saintPetersburgId = Kollege.insert {
            it[firstName_Kollege] = "$choice"
        } get Kollege.id

        val munichId = Kollege.insert {
            it[firstName_Kollege] = "Munich"
        } get Kollege.id

        Kollege.insert {
            it[firstName_Kollege] = "Prague"
        }

        Bewohner.insert {
            it[id] = "andrey"
            it[firstName_Bewohner] = "Andrey"
            it[cityId] = saintPetersburgId
        }

        Bewohner.insert {
            it[id] = "sergey"
            it[firstName_Bewohner] = "Sergey"
            it[cityId] = munichId
        }

        Bewohner.insert {
            it[id] = "eugene"
            it[firstName_Bewohner] = "Eugene"
            it[cityId] = munichId
        }

        Bewohner.insert {
            it[id] = "alex"
            it[firstName_Bewohner] = "Alex"
            it[cityId] = null
        }

        Bewohner.insert {
            it[id] = "smth"
            it[firstName_Bewohner] = "Something"
            it[cityId] = null
        }

        Bewohner.update({Bewohner.id eq "alex"}) {
            it[firstName_Bewohner] = "Alexey"
        }

        Bewohner.deleteWhere{Bewohner.firstName_Bewohner like "%thing"}

        println("All cities:")

        for (city in Kollege.selectAll()) {
            println("${city[Kollege.id]}: ${city[Kollege.firstName_Kollege]}")
        }

        println("Manual join:")
        (Bewohner innerJoin Kollege).slice(Bewohner.firstName_Bewohner, Kollege.firstName_Kollege).
            select {(Bewohner.id.eq("andrey") or Bewohner.firstName_Bewohner.eq("Sergey")) and
                    Bewohner.id.eq("sergey") and Bewohner.cityId.eq(Kollege.id)}.forEach {
            println("${it[Bewohner.firstName_Bewohner]} lives in ${it[Kollege.firstName_Kollege]}")
        }

        println("Join with foreign key:")


        (Bewohner innerJoin Kollege).slice(Bewohner.firstName_Bewohner, Bewohner.cityId, Kollege.firstName_Kollege).
            select {Kollege.firstName_Kollege.eq("St. Petersburg") or Bewohner.cityId.isNull()}.forEach {
            if (it[Bewohner.cityId] != null) {
                println("${it[Bewohner.firstName_Bewohner]} lives in ${it[Kollege.firstName_Kollege]}")
            }
            else {
                println("${it[Bewohner.firstName_Bewohner]} lives nowhere")
            }
        }

        println("Functions and group by:")

        ((Kollege innerJoin Bewohner).slice(Kollege.firstName_Kollege, Bewohner.id.count()).selectAll().groupBy(Kollege.firstName_Kollege)).forEach {
            val cityName = it[Kollege.firstName_Kollege]
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