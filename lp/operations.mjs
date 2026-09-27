export const CREATE_DISCOUNT = `mutation CreateLpDiscount($input: DiscountCodeBasicInput!) {
  discountCodeBasicCreate(basicCodeDiscount: $input) {
    codeDiscountNode { id }
    userErrors { field message code }
  }
}`;

export const FIND_DISCOUNT = `query FindLpDiscount($code: String!) {
  codeDiscountNodeByCode(code: $code) {
    codeDiscount {
      ... on DiscountCodeBasic {
        endsAt
        status
        asyncUsageCount
        usageLimit
        customerGets {
          items { __typename }
          value { ... on DiscountPercentage { percentage } }
        }
      }
    }
  }
}`;
