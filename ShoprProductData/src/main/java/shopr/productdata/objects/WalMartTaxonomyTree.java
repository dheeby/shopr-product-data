package shopr.productdata.objects;

import java.util.List;

/**
 * Created by Neil on 9/26/2016.
 * @author Neil Allison
 */
public class WalMartTaxonomyTree
{
    private List<WalMartTaxonomyTreeCategory> categories;

    public WalMartTaxonomyTree()
    {
    }

    public List<WalMartTaxonomyTreeCategory> getCategories()
    {
        return categories;
    }

    public void setCategories(List<WalMartTaxonomyTreeCategory> categories)
    {
        this.categories = categories;
    }

    private String treeToString(List<WalMartTaxonomyTreeCategory> tree, int level)
    {
        String treeString = "";
        for (WalMartTaxonomyTreeCategory walMartTaxonomyTreeCategory : tree)
        {
            for (int i = 0; i < level; i++)
            {
                treeString += "\t";
            }
            treeString += String.format("name: %s, id: %s%n", walMartTaxonomyTreeCategory.getName(), walMartTaxonomyTreeCategory.getId());

            if (!(walMartTaxonomyTreeCategory.getChildren().isEmpty()))
            {
                treeString += treeToString(walMartTaxonomyTreeCategory.getChildren(), level + 1);
            }
        }
        return treeString;
    }

    @Override
    public String toString()
    {
        return treeToString(categories, 0);
    }
}
