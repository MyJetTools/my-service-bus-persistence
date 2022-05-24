class SubPagesWidget {


    public static renderPagesWidget(page: ILoadedPage): string {

        let result = '<div class="page-widget"><svg style="font-size:16px" width="400" height="20">' +
            '<rect width="400" height="20" style="fill:none;stroke-width:;stroke:black"/>';



        for (let i of page.subPages) {
            result +=
                '<line x1="' +
                (i * 4) +
                '" y1="' +
                0 +
                '" x2="' +
                i * 4 +
                '" y2="' +
                20 +
                '" style="stroke:blue;stroke-width:2" />';
        }


        result += '</svg></div>';

        return result;

    }

}