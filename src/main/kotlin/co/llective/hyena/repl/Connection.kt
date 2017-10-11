package co.llective.hyena.repl

import co.llective.hyena.api.HyenaApi

class Connection {
    val hyena: HyenaApi

    constructor(address: String) {
        hyena = HyenaApi()
        hyena.connect(address)
    }

    fun listColumns() = hyena.listColumns()

}