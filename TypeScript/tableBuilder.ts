class TableBuilder {

    public content: String;

    public append(values: string[]) {

        if (!this.content) {
            this.content += '<table style="width:100%">';
        }


        this.content += '<tr>';
        for (let value of values) {
            this.content += '<td>' + value + '</td>';
        }


        this.content += '</tr>';
    }


    public getResult(): string {
        return this.content + '</tr></table>';
    }

}